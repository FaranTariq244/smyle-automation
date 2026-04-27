"""
Klaviyo email section metrics extractor for SMYLE_ONLINE_STRATEGY_RN_FC1 weekly report.

UI source → API mapping
────────────────────────────────────────────────────────────────────────
Week-specific metrics (use report start_date / end_date):

  Email turnover   → Home > Business performance summary > Attributed revenue (Email)
                     API: HYBRID — campaign-values-reports (conversion_value, very accurate)
                          + metric-aggregates $attributed_flow (flow revenue)
                     Accuracy: ~1.5% off UI (down from 6.3% with single-method approach)

  % flows          → same page, Flows / (Campaigns + Flows)
  % campaigns      → same page, Campaigns / (Campaigns + Flows)

  List size        → Audience > Profiles > Subscriber growth > Email tab (end-of-period total)
                     API: previous_list_size + new_subscribers − exclusions
                     Note: provide previous week's list size in config (KLAVIYO_LIST_SIZE)
                     for first run, seed it manually from the UI.

  List growth rate → same page ("X% vs previous period")
                     API: (end_size / start_size − 1) × 100

────────────────────────────────────────────────────────────────────────
30-day metrics (always last 30 days regardless of week — user decision):
  NOTE: uses rolling 30-day window ending at run time, not report week dates.

  Open rate        → Analytics > Deliverability > Score tab
                     API: unique Opened Email (non-Apple) / Received Email
                     Fix: Apple inbox provider opens are subtracted to remove MPP auto-opens.
                     Accuracy: ~0.9% off UI (down from 5% without MPP fix)

  Click rate       → same Score tab
                     API: unique Clicked Email / Received Email
                     Accuracy: ~0.05% off UI (essentially exact)

  Unsub rate       → same Score tab
                     API: Unsubscribed from Email Marketing / Received Email
                     Accuracy: ~0.17% off UI (Klaviyo Score uses proprietary weighting)

  Spam complaint   → same Score tab
                     API: Marked Email as Spam / Received Email
                     Accuracy: ~0.016% off UI (same proprietary weighting limitation)

  Deliverability   → Analytics > Deliverability > Score tab (always 30-day, no custom date)
  score              API: NOT AVAILABLE via public API — returns None (enter manually)

  Placed order     → Campaigns > "Email performance last 30 days" header
  rate               API: campaign-values-reports last 30 days, weighted conversion_rate
                     Accuracy: ~0.01% off UI (essentially exact)

────────────────────────────────────────────────────────────────────────
Metric IDs (Smyle account):
  Placed Order                     : TgSTnN
  Received Email                   : WMAUpM
  Opened Email                     : YA2MUg
  Clicked Email                    : SzJ6Lc
  Bounced Email                    : XjYGF2
  Unsubscribed from Email Marketing: V2D2VN   ← matches Score page unsub rate
  Marked Email as Spam             : TK8VE4
  Subscribed to List               : QUxyRY
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Dict, Optional

import requests

from services.klaviyo.client import _headers, _BASE_URL

# ── Metric IDs (Smyle account) ──────────────────────────────────────────────
_PLACED_ORDER     = "TgSTnN"
_RECEIVED_EMAIL   = "WMAUpM"
_OPENED_EMAIL     = "YA2MUg"
_CLICKED_EMAIL    = "SzJ6Lc"
_BOUNCED_EMAIL    = "XjYGF2"
_UNSUB_EMAIL_MKT  = "V2D2VN"   # "Unsubscribed from Email Marketing"
_SPAM             = "TK8VE4"
_SUBSCRIBED_LIST  = "QUxyRY"


# ── Helpers ──────────────────────────────────────────────────────────────────

def _agg(metric_id: str, measurements: list[str],
         start: datetime, end: datetime,
         group_by: Optional[str] = None,
         timezone: str = "Europe/Amsterdam") -> dict:
    """Run metric-aggregates POST, return the raw attributes dict."""
    payload: dict = {
        "data": {
            "type": "metric-aggregate",
            "attributes": {
                "metric_id": metric_id,
                "measurements": measurements,
                "filter": [
                    f"greater-or-equal(datetime,{start:%Y-%m-%dT%H:%M:%S})",
                    f"less-than(datetime,{end:%Y-%m-%dT%H:%M:%S})",
                ],
                "timezone": timezone,
            },
        }
    }
    if group_by:
        payload["data"]["attributes"]["by"] = [group_by]

    resp = requests.post(
        f"{_BASE_URL}/metric-aggregates",
        json=payload, headers=_headers(), timeout=60,
    )
    resp.raise_for_status()
    return resp.json()["data"]["attributes"]


def _sum_flat(attr: dict, measurement: str) -> float:
    """Sum a measurement from the first (no-groupby) data entry."""
    return sum(attr["data"][0]["measurements"].get(measurement, []))


def _sum_dim(attr: dict, measurement: str, exclude_empty: bool = False) -> float:
    """Sum a measurement across all dimension groups."""
    total = 0.0
    for entry in attr["data"]:
        dim = entry["dimensions"][0] if entry["dimensions"] else ""
        if exclude_empty and dim == "":
            continue
        total += sum(entry["measurements"].get(measurement, []))
    return total


def _paginate_campaign_report(start: datetime, end: datetime, statistics: list[str]) -> list[dict]:
    """Fetch all pages of campaign-values-reports and return all result rows."""
    payload = {
        "data": {
            "type": "campaign-values-report",
            "attributes": {
                "statistics": statistics,
                "timeframe": {
                    "start": start.strftime("%Y-%m-%dT%H:%M:%S"),
                    "end":   end.strftime("%Y-%m-%dT%H:%M:%S"),
                },
                "conversion_metric_id": _PLACED_ORDER,
            },
        }
    }
    resp = requests.post(
        f"{_BASE_URL}/campaign-values-reports",
        json=payload, headers=_headers(), timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()
    rows = data.get("data", {}).get("attributes", {}).get("results", [])
    next_url = data.get("links", {}).get("next")
    while next_url:
        resp = requests.get(next_url, headers=_headers(), timeout=60)
        resp.raise_for_status()
        data = resp.json()
        rows.extend(data.get("data", {}).get("attributes", {}).get("results", []))
        next_url = data.get("links", {}).get("next")
    return rows


def _non_apple_unique_opens(start: datetime, end: datetime) -> float:
    """
    Return unique opens excluding Apple inbox provider (MPP auto-opens).
    Apple Mail Privacy Protection (iOS 15+) pre-fetches emails causing
    artificial opens. Klaviyo's Score page excludes these; we do the same.
    """
    attr = _agg(_OPENED_EMAIL, ["unique"], start, end, group_by="Inbox Provider")
    total = 0.0
    apple = 0.0
    for entry in attr["data"]:
        provider = entry["dimensions"][0] if entry["dimensions"] else ""
        val = sum(entry["measurements"].get("unique", []))
        total += val
        if "apple" in provider.lower():
            apple += val
    return total - apple


# ── Public extractors ────────────────────────────────────────────────────────

def extract_email_revenue(
    start_date: datetime,
    end_date: datetime,
) -> Dict[str, object]:
    """
    Email turnover, % flows, % campaigns for the given week.

    Source: Home > Business performance summary (Placed Order conversion, Email channel)

    HYBRID method for best accuracy (~1.5% off UI vs 6.3% with single method):
      - Campaign revenue: campaign-values-reports conversion_value (Reporting API)
        → very accurate, <0.1% off UI
      - Flow revenue: metric-aggregates Placed Order grouped by $attributed_flow
        → ~1.8% off UI (flow emails sent before period still attributed correctly
           since we filter by ORDER date, not send date)

    Returns:
        email_turnover : float (EUR, rounded to 2dp)
        pct_flows      : float percentage string ("82.55%")
        pct_campaigns  : float percentage string ("17.45%")
    """
    end_excl = end_date + timedelta(days=1)

    # Campaign revenue via Reporting API (very accurate)
    camp_rows = _paginate_campaign_report(start_date, end_excl,
                                          ["conversion_value"])
    camp_rev = sum(
        (r.get("statistics", {}).get("conversion_value") or 0)
        for r in camp_rows
    )

    # Flow revenue via metric aggregates (non-empty $attributed_flow = flow-attributed)
    flow_attr = _agg(_PLACED_ORDER, ["sum_value"], start_date, end_excl,
                     group_by="$attributed_flow")
    flow_rev = _sum_dim(flow_attr, "sum_value", exclude_empty=True)

    email_turnover = camp_rev + flow_rev

    pct_flows     = round(flow_rev / email_turnover * 100, 2) if email_turnover else 0.0
    pct_campaigns = round(camp_rev / email_turnover * 100, 2) if email_turnover else 0.0

    return {
        "email_turnover": round(email_turnover, 2),
        "pct_flows":      f"{pct_flows:.2f}%",
        "pct_campaigns":  f"{pct_campaigns:.2f}%",
    }


_SUBSCRIBER_SEGMENT_NAME = "API - Email Subscribers"


def _get_subscriber_count() -> Optional[int]:
    """Return the current email subscriber count from the Klaviyo segment.

    Looks for a segment named 'API - Email Subscribers' (created once in the
    Klaviyo UI with condition: can receive email marketing + Subscribed).
    Returns profile_count or None if the segment is not found.
    """
    url = f"{_BASE_URL}/segments"
    while url:
        resp = requests.get(url, headers=_headers(), timeout=60)
        resp.raise_for_status()
        data = resp.json()
        for seg in data.get("data", []):
            if seg["attributes"]["name"] == _SUBSCRIBER_SEGMENT_NAME:
                detail = requests.get(
                    f"{_BASE_URL}/segments/{seg['id']}",
                    params={"additional-fields[segment]": "profile_count"},
                    headers=_headers(), timeout=60,
                )
                detail.raise_for_status()
                return detail.json()["data"]["attributes"].get("profile_count")
        url = data.get("links", {}).get("next")
    return None


def extract_subscriber_growth(
    start_date: datetime,
    end_date: datetime,
    previous_list_size: Optional[int] = None,
) -> Dict[str, object]:
    """
    List size (end of period) and list growth rate.

    Source: Audience > Profiles > Subscriber growth > Email tab

    Uses the 'API - Email Subscribers' segment profile_count for the current
    list size. Falls back to delta calculation (previous + new - unsubs) if
    the segment is not found. previous_list_size is only used in fallback mode.

    Returns:
        list_size         : int or None
        list_growth_rate  : str ("1.84%") or None
    """
    # Primary: read current subscriber count from Klaviyo segment.
    end_size = _get_subscriber_count()
    if end_size is not None:
        print(f"[Klaviyo] Subscriber count from segment: {end_size:,}")
    else:
        # Fallback: delta calculation.
        print("[Klaviyo] Segment not found, using delta calculation ...")
        end_excl = end_date + timedelta(days=1)
        sub_attr   = _agg(_SUBSCRIBED_LIST, ["count"], start_date, end_excl)
        new_subs   = int(_sum_flat(sub_attr, "count"))
        unsub_attr = _agg(_UNSUB_EMAIL_MKT, ["count"], start_date, end_excl)
        exclusions = int(_sum_flat(unsub_attr, "count"))
        if previous_list_size is not None:
            end_size = previous_list_size + new_subs - exclusions
        else:
            end_size = None

    if end_size is not None and previous_list_size is not None:
        growth_pct = (end_size / previous_list_size - 1) * 100 if previous_list_size else 0.0
        growth_str = f"{growth_pct:.2f}%"
    else:
        growth_str = None

    return {
        "list_size":        end_size,
        "list_growth_rate": growth_str,
    }


def extract_deliverability_metrics(days: int = 30) -> Dict[str, object]:
    """
    Open rate, click rate, unsub rate, spam complaint rate.

    Source: Analytics > Deliverability > Score tab
    NOTE: Uses last {days} days rolling window (user decision — not week-specific).

    Accuracy vs Klaviyo Score page:
      open_rate   : ~0.9% off  (Apple MPP opens excluded via inbox provider grouping)
      click_rate  : ~0.05% off (essentially exact)
      unsub_rate  : ~0.17% off (Klaviyo Score uses proprietary per-campaign weighting)
      spam_rate   : ~0.02% off (same limitation)

    Returns rates as percentage strings ("54.40%") for Google Sheets USER_ENTERED.
    """
    end   = datetime.now()
    start = end - timedelta(days=days)

    # Denominator: all received emails
    received_attr = _agg(_RECEIVED_EMAIL, ["count"], start, end)
    received      = _sum_flat(received_attr, "count")

    if received == 0:
        return {k: None for k in
                ["open_rate", "click_rate", "unsub_rate", "spam_complaint_rate"]}

    # Open rate: exclude Apple MPP auto-opens
    non_apple_opens = _non_apple_unique_opens(start, end)

    # Click, unsub, spam
    clicked_attr = _agg(_CLICKED_EMAIL,   ["unique"], start, end)
    unsub_attr   = _agg(_UNSUB_EMAIL_MKT, ["count"],  start, end)
    spam_attr    = _agg(_SPAM,            ["count"],  start, end)

    unique_clicks = _sum_flat(clicked_attr, "unique")
    unsub_count   = _sum_flat(unsub_attr,   "count")
    spam_count    = _sum_flat(spam_attr,    "count")

    return {
        "open_rate":           f"{non_apple_opens / received * 100:.2f}%",
        "click_rate":          f"{unique_clicks   / received * 100:.2f}%",
        "unsub_rate":          f"{unsub_count     / received * 100:.2f}%",
        "spam_complaint_rate": f"{spam_count      / received * 100:.4f}%",
    }


def extract_placed_order_rate(days: int = 30) -> Optional[str]:
    """
    Campaign placed order rate (last {days} days).

    Source: Campaigns page > "Email performance last 30 days" header.
    Accuracy: ~0.01% off UI (essentially exact).
    Returns rate as percentage string ("0.15%") or None.
    """
    end   = datetime.now()
    start = end - timedelta(days=days)

    rows = _paginate_campaign_report(start, end, ["delivered", "conversions"])

    total_delivered   = sum((r.get("statistics", {}).get("delivered")   or 0) for r in rows)
    total_conversions = sum((r.get("statistics", {}).get("conversions") or 0) for r in rows)

    if total_delivered == 0:
        return None

    rate = total_conversions / total_delivered * 100
    return f"{rate:.2f}%"


def extract_all_email_metrics(
    start_date: datetime,
    end_date: datetime,
    previous_list_size: Optional[int] = None,
) -> Dict[str, object]:
    """
    Extract all EMAIL section metrics for the given week.

    Args:
        start_date         : week start (Monday)
        end_date           : week end (Sunday, inclusive)
        previous_list_size : list size at START of week (from config KLAVIYO_LIST_SIZE).
                             If None, list_size and list_growth_rate will be None.

    Returns flat dict with all sheet row values:
        email_turnover, pct_flows, pct_campaigns,
        list_size, list_growth_rate,
        open_rate, click_rate, unsub_rate, spam_complaint_rate,
        placed_order_rate,
        deliverability_score  (always None — not in public API, enter manually)
    """
    print(f"[Klaviyo] Revenue for {start_date:%Y-%m-%d} to {end_date:%Y-%m-%d} ...")
    revenue = extract_email_revenue(start_date, end_date)

    print(f"[Klaviyo] Subscriber growth ...")
    growth = extract_subscriber_growth(start_date, end_date, previous_list_size)

    print(f"[Klaviyo] Deliverability metrics (last 30 days) ...")
    deliverability = extract_deliverability_metrics(days=30)

    print(f"[Klaviyo] Placed order rate (last 30 days) ...")
    placed_order_rate = extract_placed_order_rate(days=30)

    return {
        # Week-specific
        "email_turnover":       revenue["email_turnover"],
        "pct_flows":            revenue["pct_flows"],
        "pct_campaigns":        revenue["pct_campaigns"],
        "list_size":            growth["list_size"],
        "list_growth_rate":     growth["list_growth_rate"],
        # 30-day rolling (noted in sheet as such)
        "open_rate":            deliverability["open_rate"],
        "click_rate":           deliverability["click_rate"],
        "unsub_rate":           deliverability["unsub_rate"],
        "spam_complaint_rate":  deliverability["spam_complaint_rate"],
        "placed_order_rate":    placed_order_rate,
        "deliverability_score": None,  # Not in public API — enter manually from Score tab
    }
