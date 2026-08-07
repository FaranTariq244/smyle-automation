"""
Push track & trace codes from my-fulfilment.com into Shopify.

Smyle's 3PL (Nic. Oud) ships orders and records a PostNL barcode against each
one, but nothing ever writes that back to Shopify — a scan of ~6,000 shipped
orders found tracking on virtually none of them, so customers get a "shipped"
email with no trackable link. This closes that loop.

    my-fulfilment.com  Completed order
        Package.tnt_code = "3SOSVJ0995727"
              |  matched on reference "#SMYLE120123"
              v
    Shopify order #SMYLE120123 -> fulfilment carrying that tracking number

Per order, one of three things happens:
    * not yet fulfilled in Shopify  -> fulfillmentCreate with the tracking
    * fulfilled but no tracking     -> fulfillmentTrackingInfoUpdateV2
    * already has tracking          -> skipped

DRY RUN BY DEFAULT. Nothing is written to Shopify unless you pass --apply.

Examples:
    python sync_tracking_to_shopify.py --order '#SMYLE120579'
    python sync_tracking_to_shopify.py --created-from 2026-08-01 --pages 2
    python sync_tracking_to_shopify.py --order '#SMYLE120579' --apply
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
from collections import Counter
from datetime import date, timedelta
from pathlib import Path

from services.fulfilment import client as myf
from services.shopify import fulfillment as shopify_fulfil

log = logging.getLogger("tracking_sync")

PROJECT_ROOT = Path(__file__).resolve().parent

# Orders we never write to, regardless of what the portal says. Staff test
# orders — keep them out of any bulk run.
EXCLUDED_REFERENCES: set = set()


def _decide(state, tnt_code):
    """Work out what action an order needs. Returns (action, reason, target)."""
    if state is None:
        return "missing", "not found in Shopify", None

    for f in state["fulfillments"]:
        for t in f["trackingInfo"]:
            if (t.get("number") or "").strip() == tnt_code:
                return "skip", f"already tracked ({tnt_code})", None

    tracked = [f for f in state["fulfillments"] if f["trackingInfo"]]
    if tracked:
        existing = tracked[0]["trackingInfo"][0].get("number")
        return "conflict", f"already has different tracking ({existing})", None

    if state["fulfillable"]:
        return "create", "unfulfilled", state["fulfillable"][0]["id"]

    if state["fulfillments"]:
        return "update", "fulfilled without tracking", state["fulfillments"][0]["id"]

    return "blocked", f"no fulfillable order and no fulfilment (status {state['status']})", None


def _job(reference, tnt, shipper, state, country, tnt_url=None):
    """Build one job row from a portal shipment + its Shopify state."""
    action, reason, target = _decide(state, tnt)
    return {
        "reference": reference,
        "tnt_code": tnt,
        "tnt_url": shopify_fulfil.normalize_tracking_url(tnt_url) or "",
        "shipper": shipper,
        "carrier": shopify_fulfil.carrier_for(
            shipper, (state or {}).get("country_code") or country),
        "country": (state or {}).get("country_code") or country,
        "shopify_status": (state or {}).get("status", "-"),
        "action": action,
        "reason": reason,
        "target": target,
        "result": "",
    }


def collect_range(date_from, date_to, limit=None):
    """Every shipment with a T&T code in a window, matched to Shopify.

    Bulk on both sides: the portal is listed with a date-only filter across a
    session pool (with a local cache for Completed orders), then Shopify is
    queried only for the references that actually carry a tracking number,
    batched 40 at a time. No per-order round-trips on either side.
    """
    import order_lookup as ol
    from services.shopify import bulk

    portal_rows = ol.portal_orders_fast(date_from, date_to,
                                        only_status=ol.DETAIL_STATUS)
    tracked = [p for p in portal_rows
               if p.get("portal_tracking") and p["order"] not in EXCLUDED_REFERENCES]
    log.info("Portal: %d completed, %d with a tracking code",
             len(portal_rows), len(tracked))
    if limit:
        tracked = tracked[:limit]

    nodes = bulk.fetch_by_names([p["order"] for p in tracked], ol._ORDER_FIELDS)
    log.info("Shopify: matched %d of %d reference(s)", len(nodes), len(tracked))

    jobs = []
    for p in tracked:
        node = nodes.get(p["order"])
        if node is None:
            # Portal also holds Amazon/Kaufland orders that never reach Shopify.
            jobs.append(_job(p["order"], p["portal_tracking"].split(",")[0].strip(),
                             p.get("shipper", ""), None, p.get("portal_country", ""),
                             p.get("tracking_url")))
            continue
        state = shopify_fulfil.state_from_node(node)
        jobs.append(_job(p["order"], p["portal_tracking"].split(",")[0].strip(),
                         p.get("shipper", ""), state, p.get("portal_country", ""),
                         p.get("tracking_url")))
    return jobs


def collect(args):
    """Gather the jobs for whatever selection was requested."""
    if args.order:
        jobs = []
        with myf.MyFulfilmentClient() as portal:
            portal.login()
            for ref in args.order:
                row = portal.find_order(ref)
                if row is None:
                    log.warning("%s not found in my-fulfilment.com", ref)
                    continue
                detail = portal.get_order_detail(row)
                if not detail.tracked_packages:
                    log.warning("%s has no track & trace code (status %s)",
                                ref, detail.status)
                    continue
                for pkg in detail.tracked_packages:
                    if row.reference in EXCLUDED_REFERENCES:
                        log.info("%s excluded by config — skipping", row.reference)
                        continue
                    state = shopify_fulfil.get_order_state(row.reference)
                    jobs.append(_job(row.reference, pkg.tnt_code, pkg.shipper,
                                     state, row.country, pkg.tnt_url))
        return jobs

    return collect_range(args.created_from, args.created_to, args.limit)


def apply(jobs, notify):
    """Execute the create/update actions. Mutates each job's `result`."""
    from services.shopify import audit
    # Label these writes so the audit log distinguishes an automated sweep
    # from someone clicking the button in the UI.
    audit.set_origin("scheduled" if os.getenv("SCHEDULED_RUN") else "cli-sync")

    for job in jobs:
        if job["action"] not in ("create", "update"):
            job["result"] = f"skipped ({job['action']})"
            continue
        try:
            if job["action"] == "create":
                f = shopify_fulfil.create_fulfillment(
                    job["target"], job["tnt_code"], tracking_company=job["carrier"],
                    tracking_url=job.get("tnt_url"), notify_customer=notify)
            else:
                f = shopify_fulfil.update_tracking(
                    job["target"], job["tnt_code"], tracking_company=job["carrier"],
                    tracking_url=job.get("tnt_url"), notify_customer=notify)
            job["result"] = f"OK {f['name']}"
        except Exception as exc:
            job["result"] = f"ERROR {exc}"
            log.error("%s failed: %s", job["reference"], exc)


def report(jobs, applied):
    if not jobs:
        print("Nothing to do.")
        return
    header = f"{'REFERENCE':<16} {'TRACKING':<22} {'CARRIER':<22} {'SHOPIFY':<14} {'ACTION':<9} {'DETAIL'}"
    print("\n" + header)
    print("-" * len(header))
    for j in jobs:
        detail = j["result"] if applied else j["reason"]
        print(f"{j['reference']:<16} {j['tnt_code']:<22} "
              f"{(j['carrier'] or '(no carrier)'):<22} {j['shopify_status']:<14} "
              f"{j['action']:<9} {detail}")

    counts = Counter(j["action"] for j in jobs)
    print(f"\n{len(jobs)} shipment(s): " +
          ", ".join(f"{v} {k}" for k, v in counts.most_common()))
    if not applied:
        writes = counts["create"] + counts["update"]
        print(f"\nDRY RUN — nothing was written. {writes} order(s) would be updated.")
        print("Re-run with --apply to write them to Shopify.")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--order", action="append",
                   help="specific reference, e.g. '#SMYLE120579' (repeatable)")
    p.add_argument("--created-from", help="portal filter, YYYY-MM-DD")
    p.add_argument("--created-to", help="portal filter, YYYY-MM-DD")
    p.add_argument("--days-back", type=int,
                   help="rolling window size in days, inclusive. --days-back 5 "
                        "means 5 days ending today. Overrides "
                        "--created-from/--created-to.")
    p.add_argument("--skip-days", type=int, default=0,
                   help="exclude this many recent days from the window. "
                        "--days-back 3 --skip-days 2 covers the 3 days ending "
                        "the day before yesterday, leaving today and yesterday "
                        "to a different schedule.")
    p.add_argument("--pages", type=int, default=1,
                   help="unused on the fast path; kept for backwards compatibility")
    p.add_argument("--limit", type=int, help="stop after N shipments")
    p.add_argument("--apply", action="store_true",
                   help="actually write to Shopify (default is a dry run)")
    p.add_argument("--notify", action="store_true",
                   help="send Shopify's shipping email to the customer (default off)")
    p.add_argument("--csv", help="write the result table to this CSV path")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-7s %(message)s",
                        datefmt="%H:%M:%S")

    if args.days_back:
        # Window ends `skip_days` before today and spans `days_back` days.
        #   --days-back 2                -> yesterday .. today
        #   --days-back 3 --skip-days 2  -> the 3 days ending the day before
        #                                   yesterday (today+yesterday excluded)
        today = date.today()
        skip = max(0, args.skip_days or 0)
        end = today - timedelta(days=skip)
        args.created_to = end.isoformat()
        args.created_from = (end - timedelta(days=max(0, args.days_back - 1))).isoformat()
        log.info("Rolling window: %s .. %s (%d day(s)%s)",
                 args.created_from, args.created_to, args.days_back,
                 f", skipping the last {skip} day(s)" if skip else ", including today")

    if not args.order and not (args.created_from and args.created_to):
        log.error("Give --order, --days-back N, or both --created-from and --created-to.")
        return 2

    jobs = collect(args)

    if args.apply:
        writes = sum(1 for j in jobs if j["action"] in ("create", "update"))
        if not writes:
            print("Nothing to write.")
        else:
            log.info("Applying %d change(s) to Shopify (notify_customer=%s)",
                     writes, args.notify)
            apply(jobs, args.notify)

    report(jobs, args.apply)

    if args.csv and jobs:
        with open(args.csv, "w", newline="", encoding="utf-8-sig") as fh:
            w = csv.DictWriter(fh, fieldnames=list(jobs[0].keys()))
            w.writeheader()
            w.writerows(jobs)
        print(f"Wrote {len(jobs)} rows -> {args.csv}")

    failed = sum(1 for j in jobs if j["result"].startswith("ERROR"))
    if failed:
        log.error("%d shipment(s) failed", failed)
        return 1
    print("\nWorkflow completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
