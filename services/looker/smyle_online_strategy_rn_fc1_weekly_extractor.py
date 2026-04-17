"""
SMYLE_ONLINE_STRATEGY_RN_FC1 (Weekly) - Looker Studio data extractor.

Given a date range, scrape three values from Looker Studio:
  - "Total"     -> Net Revenue scorecard on the KPI page
  - "Recurring" -> Grand-total of "Net Rev. RP" in the Order Type table
  - "New"       -> Grand-total of "Net Rev. FT" in the same table

The two report pages belong to the SMYLE dashboard used by this report.
They are hard-coded here because the report is tied to this specific dashboard.
"""

from __future__ import annotations

import time
from datetime import datetime
from typing import Dict, Optional

from browser_manager import BrowserManager
from looker_data_extractor import LookerDataExtractor


KPI_PAGE_URL = (
    "https://datastudio.google.com/u/0/reporting/"
    "ddcef9f1-b6d4-4ed3-86e3-38c70a521a2c/page/p_87aahsekwc"
)
SUBSCRIPTIONS_PAGE_URL = (
    "https://datastudio.google.com/u/0/reporting/"
    "ddcef9f1-b6d4-4ed3-86e3-38c70a521a2c/page/h0vQC"
)


def _parse_money(value_str: Optional[str]) -> float:
    """Parse '17.35K €', '6,006 €', '11,348 €', '1.2M' etc. to float euros."""
    if not value_str:
        return 0.0
    cleaned = (
        value_str.replace("\u20ac", "")  # €
        .replace("\u00a0", " ")          # nbsp
        .replace("%", "")
        .strip()
    )
    cleaned = cleaned.replace(",", "")
    if cleaned in ("", "-"):
        return 0.0
    try:
        if cleaned.endswith("K") or cleaned.endswith("k"):
            return float(cleaned[:-1].strip()) * 1_000.0
        if cleaned.endswith("M"):
            return float(cleaned[:-1].strip()) * 1_000_000.0
        return float(cleaned)
    except ValueError:
        return 0.0


# JS to read a Looker Studio scorecard component by its KPI label.
_SCORECARD_JS = r"""
var label = arguments[0];
var cards = document.querySelectorAll('.scorecard-component');
for (var i = 0; i < cards.length; i++) {
    var card = cards[i];
    var rect = card.getBoundingClientRect();
    if (rect.width === 0 || rect.height === 0) continue;
    var valueEl = card.querySelector('.value-label');
    var labelEl = card.querySelector('.kpi-label');
    if (valueEl && labelEl) {
        var labelText = (labelEl.textContent || '').trim().replace(/\*/g, '');
        if (labelText.toLowerCase() === String(label).toLowerCase()) {
            return (valueEl.textContent || '').trim();
        }
    }
}
return null;
"""


# JS to find the Looker table whose header contains 'Net Rev. FT' / 'Net Rev. RP'
# and return those values from its 'Grand total' (totalsRow) row.
#
# Rendered structure for the table we care about:
#   div.table
#     div.headerRow > div.centerHeaderRow > div.headerCell         (13 cells)
#     div.row ...                                                   (data rows)
#     div.totalsRow > div.centerTotalsRow > div.totalsCell          (13 cells)
#                        > div.totalsContent > span.colName        (value text)
# Header[i] aligns 1:1 with totals[i]. Header[0] is "Billing Country ▼",
# totals[0] is "Grand total".
_ORDER_TYPE_TABLE_JS = r"""
var out = {net_rev_ft: null, net_rev_rp: null, headers: [], totals: [], tables_seen: 0};
var tables = document.querySelectorAll('div.table');
for (var i = 0; i < tables.length; i++) {
    var tbl = tables[i];
    if ((tbl.textContent || '').indexOf('Net Rev. FT') === -1) continue;
    out.tables_seen += 1;

    // Column headers (strip sort-arrow glyphs like ▼▲).
    var headerCells = tbl.querySelectorAll('div.headerRow div.headerCell');
    var headers = [];
    for (var j = 0; j < headerCells.length; j++) {
        var ht = (headerCells[j].textContent || '').replace(/[\u25bc\u25b2]/g, '').trim();
        headers.push(ht);
    }

    // Grand total row values.
    var tr = tbl.querySelector('div.totalsRow');
    if (!tr) continue;
    var totalsCells = tr.querySelectorAll('div.centerTotalsRow > div.totalsCell');
    var totals = [];
    for (var k = 0; k < totalsCells.length; k++) {
        var sp = totalsCells[k].querySelector('span.colName');
        totals.push(sp ? (sp.textContent || '').trim() : '');
    }

    out.headers = headers;
    out.totals = totals;

    if (headers.length !== totals.length) continue;

    for (var m = 0; m < headers.length; m++) {
        if (headers[m] === 'Net Rev. FT') out.net_rev_ft = totals[m];
        if (headers[m] === 'Net Rev. RP') out.net_rev_rp = totals[m];
    }

    if (out.net_rev_ft !== null && out.net_rev_rp !== null) return out;
}
return out;
"""


def _wait_for_login(driver, timeout: int = 300) -> None:
    if "accounts.google.com" not in driver.current_url:
        return
    print("  [Looker] Sign-in required; waiting for manual login (5 min max) ...")
    t0 = time.time()
    while (
        "accounts.google.com" in driver.current_url
        and time.time() - t0 < timeout
    ):
        time.sleep(3)


def extract_weekly_values(
    start_date: datetime,
    end_date: datetime,
    driver=None,
) -> Dict[str, Optional[float]]:
    """Scrape Total / Recurring / New from Looker Studio for the given range.

    Returns a dict with keys: 'total', 'recurring', 'new'. Any value that
    could not be read comes back as None so callers can decide whether to
    write it or leave the cell untouched.
    """
    raw: Dict[str, Optional[str]] = {
        "total_raw": None,
        "recurring_raw": None,
        "new_raw": None,
    }

    manager: Optional[BrowserManager] = None
    own_driver = driver is None
    try:
        if own_driver:
            manager = BrowserManager(use_existing_chrome=False)
            driver = manager.start_browser()

        helper = LookerDataExtractor(driver)

        # ---- Page 1: KPI scorecard "Net Revenue" ----
        print("  [Looker] Opening KPI page ...")
        driver.get(KPI_PAGE_URL)
        time.sleep(6)
        _wait_for_login(driver)

        print(
            f"  [Looker] Setting date range "
            f"{start_date:%b %d, %Y} -> {end_date:%b %d, %Y}"
        )
        helper.set_date_range(start_date, end_date)
        time.sleep(4)

        net_revenue_str = driver.execute_script(_SCORECARD_JS, "Net Revenue")
        print(f"  [Looker] Net Revenue raw: {net_revenue_str!r}")
        raw["total_raw"] = net_revenue_str

        # ---- Page 2: Order Type table "Grand total" row ----
        print("  [Looker] Opening Subscriptions page ...")
        driver.get(SUBSCRIPTIONS_PAGE_URL)
        time.sleep(6)
        _wait_for_login(driver)

        # Confirm the same date range on this page.
        helper.set_date_range(start_date, end_date)
        time.sleep(5)

        table_result = driver.execute_script(_ORDER_TYPE_TABLE_JS) or {}
        print(
            f"  [Looker] Order-type table: FT={table_result.get('net_rev_ft')!r}, "
            f"RP={table_result.get('net_rev_rp')!r}, "
            f"tables_seen={table_result.get('tables_seen')}"
        )
        raw["new_raw"] = table_result.get("net_rev_ft")
        raw["recurring_raw"] = table_result.get("net_rev_rp")
    finally:
        if own_driver and manager is not None:
            manager.close()

    def _opt(val: Optional[str]) -> Optional[float]:
        if val is None:
            return None
        return _parse_money(val)

    return {
        "total": _opt(raw["total_raw"]),
        "recurring": _opt(raw["recurring_raw"]),
        "new": _opt(raw["new_raw"]),
    }


if __name__ == "__main__":
    # Minimal standalone runner for quick manual testing.
    import sys

    def _parse(s: str) -> datetime:
        for fmt in ("%d-%b-%Y", "%Y-%m-%d", "%d-%B-%Y"):
            try:
                return datetime.strptime(s.strip(), fmt)
            except ValueError:
                continue
        raise SystemExit(f"Bad date: {s}")

    if len(sys.argv) != 3:
        print("Usage: python -m services.looker.smyle_online_strategy_rn_fc1_weekly_extractor START END")
        sys.exit(1)
    start = _parse(sys.argv[1])
    end = _parse(sys.argv[2])
    result = extract_weekly_values(start, end)
    print(result)
