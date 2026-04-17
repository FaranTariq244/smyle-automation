"""
SMYLE_ONLINE_STRATEGY_RN_FC1 (Weekly) - Marketing Deepdive extractor.

Given a date range, open the SMYLE dashboard's Marketing Deepdive page,
apply the date range, ensure the 'Medium' filter has only Facebook
selected (Google Ads unchecked), and scrape all scorecard KPIs we write
into the WEEKLY sheet:

  Looker Studio label  ->  WEEKLY sheet row
  Impressions              Impressions
  Clicks                   Clicks
  CTR                      CTR
  Conversions              ORDERS
  Conversion %             CR
  Online Revenue           TURNOVER
  Spend                    SPEND
  AOV* / AOV               AOV
  CPO * / CPO              CPO
  ROAS * / ROAS            ROAS

The three remaining sheet rows (cpc, DAILY SPEND, DAILY ORDERS) are
written as Google Sheets formulas by the report runner, not here.
"""

from __future__ import annotations

import time
from datetime import datetime
from typing import Dict, Optional

from browser_manager import BrowserManager
from looker_data_extractor import LookerDataExtractor


def switch_medium(driver, medium: str) -> None:
    """Flip the Medium filter to `medium` (Facebook or Google Ads) without scraping.

    Used to switch back to Facebook after a Google Ads scrape so subsequent
    country-filter changes operate on the Facebook scorecards.
    """
    helper = LookerDataExtractor(driver)
    print(f"  [Looker-M] Switching Medium -> {medium}")
    helper.select_medium(medium)
    time.sleep(3)


MARKETING_DEEPDIVE_PAGE_URL = (
    "https://datastudio.google.com/u/0/reporting/"
    "ddcef9f1-b6d4-4ed3-86e3-38c70a521a2c/page/M05qB"
)


# Mapping from Looker Studio's scorecard KPI label (sans trailing *) to
# the canonical metric key we return from this module.
_KPI_LABEL_TO_KEY = {
    "Impressions": "impressions",
    "Clicks": "clicks",
    "CTR": "ctr",
    "Conversions": "orders",
    "Conversion %": "cr",
    "Online Revenue": "turnover",
    "Spend": "spend",
    "AOV": "aov",
    "CPO": "cpo",
    "ROAS": "roas",
}


# JS reads every visible .scorecard-component, returns {label: valueText}.
# Caller decides how to parse the value strings. Strips the Looker "*"
# suffix so "AOV*" / "CPO *" / "ROAS *" collapse to "AOV" / "CPO" / "ROAS".
_ALL_SCORECARDS_JS = r"""
var out = {};
var cards = document.querySelectorAll('.scorecard-component');
for (var i = 0; i < cards.length; i++) {
    var card = cards[i];
    var rect = card.getBoundingClientRect();
    if (rect.width === 0 || rect.height === 0) continue;
    var valueEl = card.querySelector('.value-label');
    var labelEl = card.querySelector('.kpi-label');
    if (!valueEl || !labelEl) continue;
    var labelText = (labelEl.textContent || '').replace(/\*/g, '').trim();
    var valueText = (valueEl.textContent || '').trim();
    if (labelText && valueText && !(labelText in out)) {
        out[labelText] = valueText;
    }
}
return out;
"""


def _parse_value(raw: Optional[str]) -> Optional[float]:
    """Parse '478.75K', '4.78K', '1.00%', '11.35K €', '2.53', '-' -> float.

    Percent-suffixed values are divided by 100 so the sheet's native
    percent formatting (e.g. 0.0112 -> 1.12%) renders correctly.
    """
    if raw is None:
        return None
    is_percent = "%" in raw
    cleaned = (
        raw.replace("\u20ac", "")  # €
        .replace("\u00a0", " ")   # nbsp
        .replace("%", "")
        .strip()
    )
    cleaned = cleaned.replace(",", "")
    if cleaned in ("", "-"):
        return None
    try:
        if cleaned.endswith("K") or cleaned.endswith("k"):
            number = float(cleaned[:-1].strip()) * 1_000.0
        elif cleaned.endswith("M"):
            number = float(cleaned[:-1].strip()) * 1_000_000.0
        else:
            number = float(cleaned)
    except ValueError:
        return None
    if is_percent:
        number /= 100.0
    return number


def _wait_for_login(driver, timeout: int = 300) -> None:
    if "accounts.google.com" not in driver.current_url:
        return
    print("  [Looker-M] Sign-in required; waiting for manual login (5 min max) ...")
    t0 = time.time()
    while (
        "accounts.google.com" in driver.current_url
        and time.time() - t0 < timeout
    ):
        time.sleep(3)


def extract_weekly_marketing_values(
    start_date: datetime,
    end_date: datetime,
    driver=None,
) -> Dict[str, Optional[float]]:
    """Scrape Facebook-only marketing KPIs from Looker Studio.

    Returns a dict keyed by canonical metric name (see _KPI_LABEL_TO_KEY):
        impressions, clicks, ctr, orders, cr, turnover, spend, aov, cpo, roas

    Values that couldn't be read come back as None so callers can skip
    writing those cells.

    Also returns a '_raw' sub-dict of exactly what the scorecard JS saw,
    for logging / diagnosis.
    """
    result: Dict[str, Optional[float]] = {k: None for k in _KPI_LABEL_TO_KEY.values()}
    raw_seen: Dict[str, str] = {}

    manager: Optional[BrowserManager] = None
    own_driver = driver is None
    try:
        if own_driver:
            manager = BrowserManager(use_existing_chrome=False)
            driver = manager.start_browser()

        helper = LookerDataExtractor(driver)

        print("  [Looker-M] Opening Marketing Deepdive page ...")
        driver.get(MARKETING_DEEPDIVE_PAGE_URL)
        time.sleep(6)
        _wait_for_login(driver)

        print(
            f"  [Looker-M] Setting date range "
            f"{start_date:%b %d, %Y} -> {end_date:%b %d, %Y}"
        )
        helper.set_date_range(start_date, end_date)
        time.sleep(4)

        print("  [Looker-M] Filtering Medium -> Facebook only (unchecking Google Ads)")
        helper.select_medium("Facebook")
        time.sleep(5)

        raw_seen = driver.execute_script(_ALL_SCORECARDS_JS) or {}
        print(
            "  [Looker-M] Scorecards seen: "
            + ", ".join(f"{k}={v!r}" for k, v in raw_seen.items())
        )

        for looker_label, metric_key in _KPI_LABEL_TO_KEY.items():
            raw = raw_seen.get(looker_label)
            if raw is None:
                # Looker sometimes keeps the trailing asterisk inside the span.
                raw = raw_seen.get(looker_label + "*")
            result[metric_key] = _parse_value(raw)

    finally:
        if own_driver and manager is not None:
            manager.close()

    result["_raw"] = raw_seen  # type: ignore[assignment]
    return result


def _scrape_scorecards(driver, log_context: str = "") -> Dict[str, Optional[float]]:
    """Run the scorecard JS, parse values, return canonical-metric dict.

    Shape is identical to what the public extract_* functions return, including
    a '_raw' key with the untouched label→value text map.
    """
    result: Dict[str, Optional[float]] = {k: None for k in _KPI_LABEL_TO_KEY.values()}
    raw_seen: Dict[str, str] = driver.execute_script(_ALL_SCORECARDS_JS) or {}
    suffix = f" ({log_context})" if log_context else ""
    print(
        f"  [Looker-M] Scorecards seen{suffix}: "
        + ", ".join(f"{k}={v!r}" for k, v in raw_seen.items())
    )
    for looker_label, metric_key in _KPI_LABEL_TO_KEY.items():
        raw = raw_seen.get(looker_label)
        if raw is None:
            raw = raw_seen.get(looker_label + "*")
        result[metric_key] = _parse_value(raw)
    result["_raw"] = raw_seen  # type: ignore[assignment]
    return result


def extract_weekly_marketing_medium_values(
    driver,
    medium: str,
) -> Dict[str, Optional[float]]:
    """Switch the Medium filter and rescrape the scorecards.

    Caller is responsible for switching the medium back afterwards if the
    next phase expects a different filter. Country filter and date range
    are left untouched.
    """
    helper = LookerDataExtractor(driver)
    print(f"  [Looker-M] Switching Medium -> {medium}")
    helper.select_medium(medium)
    time.sleep(3)
    return _scrape_scorecards(driver, log_context=medium)


def extract_weekly_marketing_country_values(
    driver,
    country_name,
    exclude: bool = False,
    medium: Optional[str] = None,
) -> Dict[str, Optional[float]]:
    """Scrape Marketing Deepdive scorecards after narrowing the country filter.

    Assumes the caller already has the driver on the Marketing Deepdive
    page with the date range applied and Medium filter set to Facebook
    (unless `medium` is given — see below). This function applies the
    country filter (only the given countries checked), optionally flips
    the Medium filter, waits for reload, and re-scrapes the scorecards.

    Args:
        driver: a Selenium driver on the Marketing Deepdive page.
        country_name: a single country name or a list of country names.
        exclude: if True, treat country_name as the EXCLUDE list (REO
            mode) — every other enumerated country gets selected.
        medium: if set (e.g. "Google Ads"), swap the Medium filter after
            applying the country filter and before scraping. Leave None
            to keep whatever medium is already selected.

    Same return shape as extract_weekly_marketing_values.
    """
    helper = LookerDataExtractor(driver)

    if isinstance(country_name, str):
        label = country_name
    else:
        label = " + ".join(country_name)
    if exclude:
        print(f"  [Looker-M] Filtering Country -> all EXCEPT {label}")
    else:
        print(f"  [Looker-M] Filtering Country -> {label} only")
    helper.select_country(country_name, exclude=exclude)
    time.sleep(3)

    if medium:
        print(f"  [Looker-M] Switching Medium -> {medium}")
        helper.select_medium(medium)
        time.sleep(3)

    return _scrape_scorecards(driver, log_context=str(country_name))


if __name__ == "__main__":
    # Manual test: python -m services.looker.smyle_online_strategy_rn_fc1_weekly_marketing_extractor 06-Apr-2026 12-Apr-2026
    import sys

    def _parse(s: str) -> datetime:
        for fmt in ("%d-%b-%Y", "%Y-%m-%d", "%d-%B-%Y"):
            try:
                return datetime.strptime(s.strip(), fmt)
            except ValueError:
                continue
        raise SystemExit(f"Bad date: {s}")

    if len(sys.argv) != 3:
        print(
            "Usage: python -m services.looker.smyle_online_strategy_rn_fc1_weekly_marketing_extractor "
            "START END"
        )
        sys.exit(1)
    start = _parse(sys.argv[1])
    end = _parse(sys.argv[2])
    values = extract_weekly_marketing_values(start, end)
    for k, v in values.items():
        if k == "_raw":
            continue
        print(f"  {k:<12} = {v}")
