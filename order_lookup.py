"""
Look up orders in Shopify and my-fulfilment.com — one order, or a date range.

Read-only. This tool never writes to either system.

Answers the two questions you actually ask of an order:
    * what is its overall status, in both systems?
    * does it have a fulfilment / tracking number, or not?

    # one order, both systems side by side
    python order_lookup.py order SMYLE77964

    # a date range — Shopify is the master list, fast
    python order_lookup.py list --from 2026-08-01 --to 2026-08-06
    python order_lookup.py list --days 3

    # enrich each Shopify order with its portal status + T&T code
    # (one extra HTTP request per order — slow, so it is opt-in)
    python order_lookup.py list --days 1 --with-portal

    # the portal's own grid instead of Shopify's
    python order_lookup.py list --days 3 --source fulfilment

    # narrow down, and export
    python order_lookup.py list --days 7 --untracked-only --csv gaps.csv

Date filters are inclusive and interpreted in the shop's timezone
(Europe/Amsterdam).
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
from datetime import date, timedelta

from services.fulfilment import client as myf
from services.shopify import client as shopify

log = logging.getLogger("order_lookup")

# Shop timezone. Offsets are derived from the EU DST rule rather than a tz
# database, because zoneinfo has no tzdata on the Windows box this runs on.
SHOP_TZ = "Europe/Amsterdam"

# The portal grid pages ~10 rows at a time. Walk to exhaustion by default;
# the cap only exists so a bad filter can't loop forever.
PORTAL_PAGE_SIZE = 10
MAX_PORTAL_PAGES = 100


def _eu_last_sunday(year: int, month: int) -> date:
    """Date of the last Sunday in a month — the EU DST switch day."""
    d = date(year, month + 1, 1) - timedelta(days=1) if month < 12 else date(year, 12, 31)
    return d - timedelta(days=(d.weekday() + 1) % 7)


def shop_offset(day: date) -> str:
    """UTC offset in shop time for a given day, e.g. '+02:00'."""
    try:  # prefer the real tz database when it's installed
        from zoneinfo import ZoneInfo
        from datetime import datetime
        off = datetime(day.year, day.month, day.day, 12,
                       tzinfo=ZoneInfo(SHOP_TZ)).strftime("%z")
        return f"{off[:3]}:{off[3:]}"
    except Exception:
        pass
    # EU rule: CEST (+02:00) from the last Sunday in March to the last Sunday
    # in October, CET (+01:00) otherwise. Accurate to the day, which is all a
    # midnight range bound needs.
    start = _eu_last_sunday(day.year, 3)
    end = _eu_last_sunday(day.year, 10)
    return "+02:00" if start <= day < end else "+01:00"


def shop_timestamp(day) -> str:
    """Midnight shop-time as a full ISO timestamp, e.g. 2026-08-06T00:00:00+02:00.

    GOTCHA: Shopify's order search silently ignores a date-only upper bound.
    `created_at:<'2026-08-07'` returns orders created on the 7th anyway —
    verified against this store. Only a full ISO timestamp with an explicit
    offset filters correctly, so every bound goes through here.
    """
    d = day if isinstance(day, date) else date.fromisoformat(str(day))
    return f"{d.isoformat()}T00:00:00{shop_offset(d)}"

# ---------------------------------------------------------------------------
# Shopify reads
# ---------------------------------------------------------------------------

_ORDER_FIELDS = """
    id
    name
    createdAt
    sourceName
    cancelledAt
    displayFinancialStatus
    displayFulfillmentStatus
    currentTotalPriceSet { shopMoney { amount currencyCode } }
    customer { firstName lastName email }
    shippingAddress { name city countryCode zip }
    lineItems(first: 25) { edges { node { title quantity sku } } }
    fulfillments(first: 10) {
        id name status createdAt
        trackingInfo { company number url }
        location { name }
    }
    fulfillmentOrders(first: 10) {
        edges { node {
            id status requestStatus
            assignedLocation { name }
            supportedActions { action }
        } }
    }
"""


def _shopify_row(node: dict) -> dict:
    """Flatten a Shopify order node into the columns we report on."""
    tracking = [t for f in node["fulfillments"] for t in f["trackingInfo"]]
    numbers = [t["number"] for t in tracking if t.get("number")]
    items = [e["node"] for e in node["lineItems"]["edges"]]
    addr = node.get("shippingAddress") or {}
    cust = node.get("customer") or {}
    money = (node.get("currentTotalPriceSet") or {}).get("shopMoney") or {}

    return {
        "order": node["name"],
        "created": node["createdAt"][:16].replace("T", " "),
        "source": node.get("sourceName") or "",
        "financial": node["displayFinancialStatus"],
        "fulfillment": node["displayFulfillmentStatus"],
        "cancelled": "yes" if node.get("cancelledAt") else "",
        "total": money.get("amount", ""),
        "currency": money.get("currencyCode", ""),
        "country": addr.get("countryCode") or "",
        "city": addr.get("city") or "",
        "customer": " ".join(filter(None, [cust.get("firstName"), cust.get("lastName")])),
        "email": cust.get("email") or "",
        "units": sum(i["quantity"] for i in items),
        "items": " | ".join(f"{i['quantity']}x {i['title']}" for i in items),
        "has_tracking": "yes" if numbers else "no",
        "tracking": ", ".join(numbers),
        "carrier": ", ".join(sorted({t["company"] for t in tracking if t.get("company")})),
        "_node": node,
    }


def shopify_order(name: str):
    """Fetch a single Shopify order by name. Returns a flat row, or None."""
    data = shopify.graphql(
        "query($q: String!) { orders(first: 1, query: $q) { edges { node { %s } } } }"
        % _ORDER_FIELDS,
        {"q": f"name:{name.lstrip('#')}"},
    )
    edges = data["orders"]["edges"]
    return _shopify_row(edges[0]["node"]) if edges else None


def shopify_orders(date_from=None, date_to=None, limit=None) -> list:
    """Fetch Shopify orders created in a date range, newest first.

    Bounds are inclusive and expressed in shop time — see shop_timestamp() for
    why they must be full ISO timestamps rather than plain dates.
    """
    clauses = []
    if date_from:
        clauses.append(f"created_at:>='{shop_timestamp(date_from)}'")
    if date_to:
        # Make --to inclusive: bound at midnight of the following day.
        end = (date.fromisoformat(date_to) + timedelta(days=1)).isoformat()
        clauses.append(f"created_at:<'{shop_timestamp(end)}'")
    query = " AND ".join(clauses)

    page_query = """
    query($q: String!, $after: String) {
      orders(first: 250, query: $q, sortKey: CREATED_AT, reverse: true, after: $after) {
        pageInfo { hasNextPage endCursor }
        edges { node { %s } }
      }
    }
    """ % _ORDER_FIELDS

    # Split the window into per-day queries run concurrently — cursor pages
    # can't be parallelised, but independent date ranges can (3.9x measured).
    from services.shopify import bulk

    if date_from and date_to:
        nodes = bulk.fetch_range(date_from, date_to, _ORDER_FIELDS, shop_timestamp)
        rows = [_shopify_row(n) for n in nodes]
        if limit and len(rows) > limit:
            log.warning("Stopped at --limit %d; %d orders match this range.",
                        limit, len(rows))
            return rows[:limit]
        return rows

    # Open-ended window (only one bound given) — fall back to cursor paging.
    rows, after, page = [], None, 0
    while True:
        conn = shopify.graphql(page_query, {"q": query, "after": after})["orders"]
        rows += [_shopify_row(e["node"]) for e in conn["edges"]]
        page += 1
        if limit and len(rows) >= limit:
            log.warning("Stopped at --limit %d; more orders match this range.", limit)
            return rows[:limit]
        if not conn["pageInfo"]["hasNextPage"]:
            if page > 1:
                log.info("Walked %d Shopify page(s), %d order(s).", page, len(rows))
            return rows
        after = conn["pageInfo"]["endCursor"]
        log.info("  Shopify page %d — %d order(s) so far…", page, len(rows))


# ---------------------------------------------------------------------------
# Portal reads
# ---------------------------------------------------------------------------


def _portal_row(row, detail=None) -> dict:
    out = {
        "order": row.reference,
        "portal_status": row.status,
        "wms_ref": row.wms_reference,
        "in_wms": "yes" if row.is_in_wms else "no",
        "portal_country": row.country,
        "portal_created": row.created_at,
        "portal_modified": row.modified_at,
        "portal_tracking": "",
        "tracking_url": "",
        "shipper": "",
        "delivery_mode": "",
        "portal_error": "",
    }
    if detail is not None:
        pkgs = detail.tracked_packages
        out["portal_tracking"] = ", ".join(p.tnt_code for p in pkgs)
        # The portal's deep link already carries barcode + postcode + country.
        out["tracking_url"] = pkgs[0].tnt_url if pkgs else ""
        out["shipper"] = ", ".join(sorted({p.shipper for p in pkgs})) or \
            detail.fields.get("Shipper", "")
        out["delivery_mode"] = detail.fields.get("Delivery mode", "")
        out["portal_error"] = detail.error_message
    return out


def portal_order(client, reference: str, use_cache=True):
    """Fetch one portal order plus its detail. Returns a flat row, or None.

    Tries hardest to avoid `find_order()`, whose reference filter is an
    unindexed substring scan — measured at 6.9-7.9s warm, and documented at
    ~130s cold. It is the one portal call that regularly exceeds the timeout.

    Order of preference:
      1. the local cache (free) — populated by any earlier range query
      2. the cached detail_url, re-fetched directly (~0.25s)
      3. the substring search, as a last resort
    """
    from services.fulfilment import cache

    if use_cache:
        hit = cache.get_many([reference]).get(reference)
        if hit:
            row = {"order": reference, "in_wms": "yes" if hit.get("wms_ref") else "no"}
            row.update({k: v for k, v in hit.items() if k != "detail_url"})
            return row

    row = client.find_order(reference)
    if row is None:
        return None
    detail = client.get_order_detail(row)
    if use_cache:
        cache.put(reference, detail, row)
    return _portal_row(row, detail)


# Only Completed orders have a packages table, so they're the only ones worth
# spending a detail request on.
DETAIL_STATUS = "Completed"


def portal_orders_fast(date_from, date_to, only_status=None, sessions=None,
                       use_cache=True, progress=None) -> list:
    """Portal orders for a window, using the session pool + local cache.

    Replaces the old one-order-at-a-time path. Three things make it quick:
      * the grid is filtered by DATE ONLY — 0.95s/page, against 6.2s once
        statusCode and the sort are added; both are applied in Python instead
      * pages and details run across independent portal sessions (2.4x — one
        shared session gains nothing, PHP serialises it)
      * Completed details come from the local cache on repeat ranges

    Detail pages are only fetched for Completed orders, so the expensive step
    stays gated — just locally rather than server-side.
    """
    from services.fulfilment import cache
    from services.fulfilment.pool import SessionPool, DEFAULT_SESSIONS

    with SessionPool(sessions or DEFAULT_SESSIONS) as pool:
        grid = pool.list_range(
            date_from, date_to,
            progress=(lambda n: progress("list", n, 0)) if progress else None)
        log.info("Portal: %d order(s) in range", len(grid))

        keep = [r for r in grid if not only_status or r.status == only_status]
        needs_detail = [r for r in keep if r.status == DETAIL_STATUS]

        cached = cache.get_many([r.reference for r in needs_detail]) if use_cache else {}
        missing = [r for r in needs_detail if r.reference not in cached]
        log.info("Portal: %d completed — %d cached, %d to fetch",
                 len(needs_detail), len(cached), len(missing))

        details = pool.details(
            missing,
            progress=(lambda d, t: progress("detail", d, t)) if progress else None)
        if use_cache:
            # Pass the grid rows too, so the cache keeps each order's detail_url
            # and a later single-order lookup can skip the slow reference search.
            cache.put_many(details, {r.reference: r for r in missing})

    rows = []
    for r in keep:
        row = _portal_row(r)
        if r.reference in cached:
            row.update(cached[r.reference])
        elif r.reference in details:
            row.update(_portal_row(r, details[r.reference]))
        elif r.status == DETAIL_STATUS:
            # Fetched and failed — say so rather than implying "no tracking".
            row["portal_error"] = "detail unavailable"
        rows.append(row)
    return rows


def portal_orders(client, date_from=None, date_to=None, pages=None,
                  status=None, with_tracking=False) -> list:
    """Walk the portal grid for a date range, newest first.

    The portal pages ~10 orders at a time and publishes no total count, so by
    default we keep walking until it runs out (iter_orders stops on an empty or
    repeated page). `pages` caps that; hitting the cap is logged loudly, since
    a silent truncation here reads as "there were no more orders".
    """
    cap = pages or MAX_PORTAL_PAGES
    rows, seen_pages = [], 0
    for row in client.iter_orders(max_pages=cap, status=status,
                                  created_from=date_from, created_to=date_to,
                                  sort_by="created", sort_order="DESC"):
        detail = client.get_order_detail(row) if with_tracking else None
        rows.append(_portal_row(row, detail))
        if len(rows) % PORTAL_PAGE_SIZE == 0:
            seen_pages += 1
            log.info("  portal page %d — %d order(s) so far", seen_pages, len(rows))

    if len(rows) >= cap * PORTAL_PAGE_SIZE:
        log.warning("Stopped at the %d-page cap (%d orders) — there may be MORE. "
                    "Raise --pages to see the rest.", cap, len(rows))
    return rows


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------


def print_order_detail(shop_row, portal_row, reference):
    print(f"\n{'=' * 74}\n  {reference}\n{'=' * 74}")

    print("\n-- SHOPIFY " + "-" * 62)
    if shop_row is None:
        print("  not found")
    else:
        n = shop_row["_node"]
        for label, key in (("Created", "created"), ("Source", "source"),
                           ("Financial status", "financial"),
                           ("Fulfillment status", "fulfillment"),
                           ("Customer", "customer"), ("Email", "email"),
                           ("Ship to", "city")):
            print(f"  {label:<20} {shop_row[key]}")
        print(f"  {'Country':<20} {shop_row['country']}")
        print(f"  {'Total':<20} {shop_row['total']} {shop_row['currency']}")
        if shop_row["cancelled"]:
            print(f"  {'CANCELLED':<20} yes")

        print("\n  Line items:")
        for e in n["lineItems"]["edges"]:
            i = e["node"]
            print(f"    {i['quantity']}x  {i['sku'] or '-':<24} {i['title']}")

        print("\n  Fulfilments:")
        if not n["fulfillments"]:
            print("    none")
        for f in n["fulfillments"]:
            loc = (f.get("location") or {}).get("name", "?")
            print(f"    {f['name']}  {f['status']}  @ {loc}  "
                  f"{f['createdAt'][:16].replace('T', ' ')}")
            if not f["trackingInfo"]:
                print("      tracking: NONE")
            for t in f["trackingInfo"]:
                print(f"      tracking: {t['number']}  "
                      f"carrier={t['company'] or '(none)'}")
                if t.get("url"):
                    print(f"                {t['url']}")

        print("\n  Fulfilment orders:")
        fos = [e["node"] for e in n["fulfillmentOrders"]["edges"]]
        if not fos:
            print("    none")
        for fo in fos:
            actions = [a["action"] for a in fo["supportedActions"]]
            print(f"    {fo['id'].split('/')[-1]}  {fo['status']}/{fo['requestStatus']}  "
                  f"@ {(fo.get('assignedLocation') or {}).get('name', '?')}")
            print(f"      actions: {', '.join(actions) or 'none'}")

    print("\n-- MY-FULFILMENT.COM " + "-" * 52)
    if portal_row is None:
        print("  not found")
    else:
        for label, key in (("Status", "portal_status"), ("Nic. Oud ref", "wms_ref"),
                           ("In WMS", "in_wms"), ("Country", "portal_country"),
                           ("Created", "portal_created"), ("Modified", "portal_modified"),
                           ("Shipper", "shipper"), ("Delivery mode", "delivery_mode")):
            print(f"  {label:<20} {portal_row[key]}")
        print(f"  {'T&T code':<20} {portal_row['portal_tracking'] or 'NONE'}")
        if portal_row["portal_error"]:
            print(f"  {'SOFT ERROR':<20} {portal_row['portal_error']}")

    if shop_row is not None and portal_row is not None:
        print("\n-- VERDICT " + "-" * 62)
        tnt = portal_row["portal_tracking"]
        if tnt and shop_row["has_tracking"] == "no":
            print(f"  GAP — portal has {tnt}, Shopify has no tracking.")
        elif tnt and tnt in shop_row["tracking"]:
            print("  In sync — same tracking number in both systems.")
        elif tnt and shop_row["has_tracking"] == "yes":
            print(f"  MISMATCH — portal {tnt}, Shopify {shop_row['tracking']}.")
        elif not tnt:
            print("  Portal has no T&T code — nothing to sync "
                  "(untracked method, cancelled, or not shipped yet).")
    print()


LIST_COLUMNS = [
    ("order", 15), ("created", 17), ("source", 10), ("fulfillment", 13),
    ("has_tracking", 9), ("tracking", 22), ("country", 4),
]
PORTAL_COLUMNS = [("portal_status", 16), ("portal_tracking", 20)]


def print_list(rows, columns):
    if not rows:
        print("No orders found.")
        return
    header = "  ".join(f"{name.upper():<{w}}" for name, w in columns)
    print("\n" + header)
    print("-" * len(header))
    for r in rows:
        print("  ".join(f"{str(r.get(name, ''))[:w]:<{w}}" for name, w in columns))
    print(f"\n{len(rows)} order(s).")


def _exportable(rows):
    """Strip the private _node payload before writing anywhere."""
    return [{k: v for k, v in r.items() if not k.startswith("_")} for r in rows]


def write_csv(rows, path):
    clean = _exportable(rows)
    with open(path, "w", newline="", encoding="utf-8-sig") as fh:
        w = csv.DictWriter(fh, fieldnames=list(clean[0].keys()))
        w.writeheader()
        w.writerows(clean)
    print(f"Wrote {len(clean)} rows -> {path}")


def write_sheet(rows, tab, sheet_url=None):
    # Imported lazily so the whole tool doesn't need gspread/auth.json just to
    # print an order to the terminal.
    from services.sheets import order_export

    url = order_export.export_orders(_exportable(rows), tab, sheet_url)
    print(f"Wrote {len(rows)} rows -> Google Sheet tab '{tab}'\n  {url}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def cmd_order(args) -> int:
    ref = args.reference if args.reference.startswith("#") else f"#{args.reference}"

    shop_row = None
    if args.source in ("both", "shopify"):
        shop_row = shopify_order(ref)

    portal_row = None
    if args.source in ("both", "fulfilment"):
        with myf.MyFulfilmentClient() as c:
            c.login()
            portal_row = portal_order(c, ref)

    print_order_detail(shop_row, portal_row, ref)

    merged = {**(shop_row or {"order": ref}), **(portal_row or {})}
    if args.csv:
        write_csv([merged], args.csv)
    if args.sheet:
        write_sheet([merged], args.sheet_tab, args.sheet_url)
    return 0 if (shop_row or portal_row) else 1


def cmd_list(args) -> int:
    date_from, date_to = args.date_from, args.date_to
    if args.days:
        date_to = date.today().isoformat()
        date_from = (date.today() - timedelta(days=args.days - 1)).isoformat()
    if not date_from and not date_to:
        print("Give a range: --from/--to or --days N", file=sys.stderr)
        return 2

    log.info("Range %s .. %s", date_from or "-", date_to or "-")

    if args.source == "fulfilment":
        with myf.MyFulfilmentClient() as c:
            c.login()
            rows = portal_orders(c, date_from, date_to, pages=args.pages,
                                 status=args.status, with_tracking=True)
        columns = [("order", 15), ("portal_created", 19), ("portal_status", 16),
                   ("in_wms", 6), ("portal_tracking", 20), ("portal_country", 12)]
    else:
        rows = shopify_orders(date_from, date_to, limit=args.limit)
        columns = list(LIST_COLUMNS)

        if args.with_portal:
            log.info("Enriching %d order(s) from the portal — one request each…",
                     len(rows))
            with myf.MyFulfilmentClient() as c:
                c.login()
                for i, r in enumerate(rows, 1):
                    pr = portal_order(c, r["order"])
                    r.update(pr or {"portal_status": "not found"})
                    if i % 25 == 0:
                        log.info("  %d/%d", i, len(rows))
            columns += PORTAL_COLUMNS

    if args.untracked_only:
        rows = [r for r in rows if r.get("has_tracking", "no") == "no"]
    if args.unfulfilled_only:
        rows = [r for r in rows if r.get("fulfillment") != "FULFILLED"]

    print_list(rows, columns)

    if rows and args.source != "fulfilment":
        tracked = sum(1 for r in rows if r.get("has_tracking") == "yes")
        fulfilled = sum(1 for r in rows if r.get("fulfillment") == "FULFILLED")
        print(f"fulfilled {fulfilled}/{len(rows)}  |  with tracking {tracked}/{len(rows)}")
        if args.with_portal:
            gaps = sum(1 for r in rows
                       if r.get("portal_tracking") and r.get("has_tracking") == "no")
            print(f"portal has a T&T code but Shopify does not: {gaps}")

    if args.csv and rows:
        write_csv(rows, args.csv)
    if args.sheet and rows:
        tab = args.sheet_tab or _default_tab(args.source, date_from, date_to)
        write_sheet(rows, tab, args.sheet_url)
    return 0


def _default_tab(source, date_from, date_to) -> str:
    span = date_from if date_from == date_to else f"{date_from}_{date_to}"
    prefix = "portal" if source == "fulfilment" else "orders"
    return f"{prefix}_{span}"


def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="command", required=True)

    one = sub.add_parser("order", help="one order, in both systems")
    one.add_argument("reference", help="e.g. SMYLE77964 or '#SMYLE77964'")
    one.add_argument("--source", choices=["both", "shopify", "fulfilment"],
                     default="both")
    one.add_argument("--csv", help="write the row to this CSV path")
    one.add_argument("--sheet", action="store_true",
                     help="append to Google Sheets (ORDER_EXPORT_SHEET_URL)")
    one.add_argument("--sheet-tab", default="order_lookups",
                     help="worksheet tab name (default: order_lookups)")
    one.add_argument("--sheet-url", help="override the configured spreadsheet URL")

    many = sub.add_parser("list", help="orders in a date range")
    many.add_argument("--from", dest="date_from", help="YYYY-MM-DD (inclusive)")
    many.add_argument("--to", dest="date_to", help="YYYY-MM-DD (inclusive)")
    many.add_argument("--days", type=int, help="last N days, ending today")
    many.add_argument("--source", choices=["shopify", "fulfilment"], default="shopify")
    many.add_argument("--with-portal", action="store_true",
                      help="add portal status + T&T per order (slow)")
    many.add_argument("--status", help="portal status filter (fulfilment source only)")
    many.add_argument("--pages", type=int,
                      help="cap portal grid pages (fulfilment source only; "
                           "default walks until the range is exhausted)")
    many.add_argument("--limit", type=int,
                      help="cap the number of Shopify orders returned")
    many.add_argument("--untracked-only", action="store_true",
                      help="only orders with no tracking in Shopify")
    many.add_argument("--unfulfilled-only", action="store_true",
                      help="only orders not FULFILLED in Shopify")
    many.add_argument("--csv", help="write results to this CSV path")
    many.add_argument("--sheet", action="store_true",
                      help="export to Google Sheets (ORDER_EXPORT_SHEET_URL)")
    many.add_argument("--sheet-tab", help="worksheet tab name (default: orders_<range>)")
    many.add_argument("--sheet-url", help="override the configured spreadsheet URL")

    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")

    return cmd_order(args) if args.command == "order" else cmd_list(args)


if __name__ == "__main__":
    raise SystemExit(main())
