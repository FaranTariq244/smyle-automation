"""
DataAds report extraction via the public API.

Drop-in replacement for the Selenium path in datads_data_extractor.py. It
exposes the same two entry points with the same signatures and return values:

    run_datads_report(date_obj, date_str) -> bool
    run_datads_weekly_report(start, end, start_str, end_str) -> bool

and emits rows in the identical shape the sheet writers already consume:

    {"Landing page": "<url>", "<UI metric label>": "<numeric string>", ...}

The UI labels are load-bearing: services/sheets/datads_helpers.py maps them to
sheet columns via the user-editable DATADS_DAILY_MAPPINGS / DATADS_WEEKLY_MAPPINGS
settings, so METRIC_FIELDS below must keep using the exact strings the rendered
cards used.

Verified against the scraper on 2026-08-07 and 2026-08-08: 9 landing pages and
171 metric values per day, all identical — except that large numbers are no
longer abbreviated (see _format).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from services.datads import client

# UI card label -> (API metric id, format kind)
#
# Format kinds mirror how the DataAds UI rendered each metric, because
# parse_value() in the sheet helpers consumes these strings:
#   'int'    whole number
#   'money'  2 decimals (currency symbol omitted; parse_value strips it anyway)
#   'ratio'  2 decimals (ROAS-style multiplier)
#   'pct'    API returns a 0-1 fraction; the UI showed it x100
#   'pct_pt' API already returns percentage points; the UI showed it as-is
METRIC_FIELDS: List[tuple] = [
    ("Conversion Rate",                         "conversion_rate",            "pct"),
    ("CPM",                                     "cpm",                        "money"),
    ("Landing Page Views",                      "landing_page_view",          "int"),
    ("Cost per Landing Page Views",             "cost_per_landing_page_view", "money"),
    ("CPC",                                     "cpc",                        "money"),
    ("CTR (Link Click Rate)",                   "ctr",                        "pct_pt"),
    ("Add to Cart / Clicks",                    "add_to_cart_per_clicks",     "pct"),
    ("Add to Cart",                             "add_to_cart",                "int"),
    ("Initiate Checkout",                       "initiate_checkout",          "int"),
    ("Purchase / Add to Cart",                  "purchase_per_add_to_cart",   "pct"),
    ("Purchases",                               "purchases",                  "int"),
    ("Purchase / Clicks",                       "purchase_per_clicks",        "pct"),
    ("Purchase ROAS",                           "purchase_roas",              "ratio"),
    ("Cost per Purchase",                       "cost_per_purchase",          "money"),
    ("Average Order Value (AOV)",               "aov",                        "money"),
    ("Spend",                                   "spend",                      "money"),
    ("Cost per Add to Cart",                    "cost_per_add_to_cart",       "money"),
    ("NC-ROAS (Triple Attribution + Views-LT)", "custom_ncroas_tapv",         "ratio"),
    ("ROAS (Triple Attribution + Views-LT)",    "custom_roas_tapv",           "ratio"),
]

# The breakdown endpoint accepts at most 25 metrics per call.
METRIC_IDS = [api_id for _, api_id, _ in METRIC_FIELDS]
assert len(METRIC_IDS) <= 25, "DatAds breakdown accepts at most 25 metrics"


def _format(value: Any, kind: str) -> str:
    """
    Render an API number the way the UI card did — minus the 'k'/'M'
    abbreviation, which was silently costing us up to 33% on counts
    ('1k' was written to the sheet as 1000 when the real figure was 1487).
    """
    if value is None:
        return ""
    try:
        num = float(value)
    except (TypeError, ValueError):
        return ""

    if kind == "int":
        return str(int(round(num)))
    if kind == "pct":
        return f"{num * 100:.2f}"
    # money, ratio and pct_pt are all plain 2-decimal numbers
    return f"{num:.2f}"


def _unwrap(metric_value: Any) -> Any:
    """
    The breakdown endpoint returns bare numbers; the saved-report endpoint wraps
    each metric as {curValue, compareValue, percentageChange}. Accept both.
    """
    if isinstance(metric_value, dict):
        return metric_value.get("curValue")
    return metric_value


def fetch_rows(start_date: datetime, end_date: Optional[datetime] = None) -> List[Dict[str, str]]:
    """
    Fetch DataAds landing-page rows for a date range via the API.

    Report configuration (provider, grouping, spend filter) is read from the
    saved report so UI edits keep flowing through; only the dates come from us.
    """
    if end_date is None:
        end_date = start_date

    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    print(f"[API] Reading report config (report {client.get_report_id()})...")
    config = client.get_report_config()
    provider = config.get("provider") or "META"
    group_by = config.get("groupBy") or "LANDING_PAGE"
    global_filter = config.get("globalFilter")

    print(f"[API] Report '{config.get('title', '').strip()}' — provider={provider}, "
          f"groupBy={group_by}, filter={global_filter}")
    print(f"[API] Fetching breakdown for {start_str} to {end_str}...")

    groups = client.breakdown(
        provider=provider,
        metrics=METRIC_IDS,
        start_date=start_str,
        end_date=end_str,
        group_by=group_by,
        filter_=global_filter,
    )
    print(f"[API] Received {len(groups)} groups.")

    rows: List[Dict[str, str]] = []
    for group in groups:
        metrics = group.get("metrics") or {}
        row: Dict[str, str] = {"Landing page": group.get("displayName") or "Unknown"}
        for label, api_id, kind in METRIC_FIELDS:
            row[label] = _format(_unwrap(metrics.get(api_id)), kind)
        rows.append(row)

    # The API returns groups in its own order; the UI sorted by the report's
    # primary metric. Sort by spend descending so console output and any
    # order-sensitive downstream reading stay stable run to run.
    rows.sort(key=lambda r: float(r.get("Spend") or 0), reverse=True)
    return rows


def display_data(data: List[Dict[str, str]]) -> None:
    """Print extracted rows for console verification (mirrors the UI extractor)."""
    if not data:
        print("\nNo data to display.")
        return

    print("\n" + "=" * 100)
    print(f"EXTRACTED DATA ({len(data)} rows)")
    print("=" * 100)
    for i, row in enumerate(data):
        print(f"\n--- Row {i + 1} ---")
        for key, value in row.items():
            print(f"  {key}: {value}")
    print("\n" + "=" * 100)
    print(f"Total rows: {len(data)}")
    print("=" * 100)


def run_datads_report(date_obj, date_str) -> bool:
    """
    Run the DataAds daily report via the API.

    Same signature and return contract as the Selenium version.
    """
    print("\n\n")
    print("=" * 80)
    print("DATADS REPORT (API)".center(80))
    print(f"Date: {date_str}".center(80))
    print("=" * 80 + "\n")

    try:
        print("[1/3] Fetching data from DatAds API...")
        data = fetch_rows(date_obj)

        if not data:
            print("No rows returned for this date — nothing to write.")
            return False

        print("\n*** DATADS REPORT DATA ***")
        display_data(data)

        print("\n" + "=" * 80)
        print("EXTRACTION SUMMARY")
        print("=" * 80)
        print(f"DataAds Report: {len(data)} rows extracted")
        print("=" * 80)

        print("\n[2/3] Writing to Google Sheets...")
        try:
            from services.sheets.datads_helpers import write_datads_data_to_sheets
            write_datads_data_to_sheets(date_obj, data)
            print("  Successfully wrote to Google Sheets!")
        except Exception as e:
            print(f"  Warning: Could not write to Google Sheets: {e}")
            print("  (Data was extracted successfully, but sheet update failed)")
            import traceback
            traceback.print_exc()

        print("\n[3/3] Done.")
        print("\n" + "=" * 80)
        print("DATADS REPORT COMPLETED SUCCESSFULLY")
        print("=" * 80)
        return True

    except Exception as e:
        print(f"\nDataAds Report failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def run_datads_weekly_report(start_date_obj, end_date_obj, start_date_str, end_date_str) -> bool:
    """
    Run the DataAds weekly (date-range) report via the API.

    Same signature and return contract as the Selenium version.
    """
    print("\n\n")
    print("=" * 80)
    print("DATADS WEEKLY REPORT (API)".center(80))
    print(f"Date range: {start_date_str} to {end_date_str}".center(80))
    print("=" * 80 + "\n")

    try:
        print("[1/3] Fetching data from DatAds API...")
        data = fetch_rows(start_date_obj, end_date_obj)

        if not data:
            print("No rows returned for this range — nothing to write.")
            return False

        print("\n*** DATADS WEEKLY REPORT DATA ***")
        display_data(data)

        print("\n" + "=" * 80)
        print("EXTRACTION SUMMARY")
        print("=" * 80)
        print(f"DataAds Weekly Report: {len(data)} rows extracted")
        print("=" * 80)

        print("\n[2/3] Writing weekly data to Google Sheets...")
        try:
            from services.sheets.datads_helpers import write_datads_weekly_data_to_sheets
            write_datads_weekly_data_to_sheets(start_date_obj, end_date_obj, data)
            print("  Successfully wrote weekly data to Google Sheets!")
        except Exception as e:
            print(f"  Warning: Could not write to Google Sheets: {e}")
            print("  (Data was extracted successfully, but sheet update failed)")
            import traceback
            traceback.print_exc()

        print("\n[3/3] Done.")
        print("\n" + "=" * 80)
        print("DATADS WEEKLY REPORT COMPLETED SUCCESSFULLY")
        print("=" * 80)
        return True

    except Exception as e:
        print(f"\nDataAds Weekly Report failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    """Fetch and print rows for a date without touching Google Sheets."""
    import sys

    if len(sys.argv) > 1:
        start = datetime.strptime(sys.argv[1], "%Y-%m-%d")
    else:
        from datetime import timedelta
        start = datetime.now() - timedelta(days=1)
    end = datetime.strptime(sys.argv[2], "%Y-%m-%d") if len(sys.argv) > 2 else start

    display_data(fetch_rows(start, end))
