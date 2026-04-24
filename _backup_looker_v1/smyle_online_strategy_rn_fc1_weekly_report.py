"""
SMYLE_ONLINE_STRATEGY_RN_FC1 (Weekly) report runner.

Each Marketing-Deepdive phase below scrapes Facebook first (META rows),
then — where the sheet has a matching GOOGLE section — flips the Medium
filter to Google Ads, rescrapes, writes the GOOGLE section, and flips
back to Facebook so the next phase's country change still operates on
Facebook scorecards.

Phase 1: ensure the WEEKLY tab has a column for the selected week under the
         right month (creating the label if necessary).
Phase 2: scrape Net Revenue / Net Rev. FT / Net Rev. RP from Looker Studio
         and write them into the 'Total', 'New', 'Recurring' rows.
Phase 3: scrape Marketing Deepdive KPIs (Facebook, no country filter) and
         write them into the META section, then scrape Google Ads and
         write the GOOGLE section.
Phase 4: narrow the Country filter to Netherlands and write META NL.
Phase 5: narrow the Country filter to Belgium and write META BE.
Phase 5b: set Country to Netherlands+Belgium, flip Medium to Google Ads,
         and write GOOGLE NL/BE. (There is no META NL/BE section, so this
         cannot ride along with Phase 4 or 5.)
Phase 6: widen the Country filter to Germany + Austria + Switzerland and
         write META DE/AU/SW, then GOOGLE DE/AU/SW.
Phase 7: invert the Country filter to "all countries EXCEPT Netherlands,
         United Kingdom, Germany, Belgium, Switzerland, Austria"
         (Rest-of-Europe) and write REO, then GOOGLE REO.
Phase 8: narrow the Country filter to United Kingdom and write META UK,
         then GOOGLE UK.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from browser_manager import BrowserManager

from services.sheets.smyle_online_strategy_rn_fc1_weekly_helpers import (
    col_index_to_letter,
    find_label_rows,
    find_or_create_week_column,
    find_section_row,
    get_weekly_worksheet,
    write_weekly_totals,
)
from services.looker.smyle_online_strategy_rn_fc1_weekly_extractor import (
    extract_weekly_values,
)
from services.looker.smyle_online_strategy_rn_fc1_weekly_marketing_extractor import (
    extract_weekly_marketing_country_values,
    extract_weekly_marketing_medium_values,
    extract_weekly_marketing_values,
    switch_medium,
)


# Scraper metric key -> WEEKLY sheet row label for the META (Facebook-all) section.
_META_METRIC_TO_ROW = {
    "impressions": "Impressions",
    "clicks": "Clicks",
    "ctr": "CTR",
    "orders": "ORDERS",
    "cr": "CR",
    "turnover": "TURNOVER",
    "spend": "SPEND",
    "aov": "AOV",
    "cpo": "CPO",
    "roas": "ROAS",
}

# Scraper metric key -> WEEKLY sheet row label for the per-country META
# sections (META NL, META BE, ...). CPO from Looker Studio maps to the
# CPA row in these sections.
_META_COUNTRY_METRIC_TO_ROW = {
    "impressions": "impressions",
    "clicks": "clicks",
    "ctr": "CTR",
    "orders": "ORDERS",
    "cr": "CR",
    "turnover": "TURNOVER",
    "spend": "SPEND",
    "aov": "AOV",
    "cpo": "CPA",
    "roas": "ROAS",
}

# Scraper metric key -> WEEKLY sheet row label for the GOOGLE (all-countries)
# section. No Clicks / Impressions / CPC rows exist here — the sheet only
# tracks spend-derived KPIs for the Google-Ads-wide total.
_GOOGLE_METRIC_TO_ROW = {
    "turnover": "Google Turnover",
    "spend": "Spend",
    "cpo": "CPO",
    "aov": "AOV",
    "roas": "ROAS",
    "cr": "CR",
    "ctr": "CTR",
    "orders": "ORDERS",
}

# Scraper metric key -> WEEKLY sheet row label for the per-country GOOGLE
# sections (GOOGLE NL/BE, GOOGLE DE/AU/SW, GOOGLE REO, GOOGLE UK). Looker's
# CPO maps to CPA here (same convention as META per-country sections).
_GOOGLE_COUNTRY_METRIC_TO_ROW = {
    "turnover": "Google Turnover",
    "spend": "Spend",
    "cpo": "CPA",
    "aov": "AOV",
    "roas": "ROAS",
    "cr": "CR",
    "clicks": "clicks",
    "ctr": "CTR",
    "orders": "ORDERS",
}


def _build_meta_writes(
    scraped: dict,
    worksheet,
    column_index: int,
    section_start_row: int,
) -> dict:
    """Build {sheet_label: value_or_formula} for the META (Facebook-all) section.

    Adds cpc / DAILY SPEND / DAILY ORDERS as Google Sheets formulas that
    reference SPEND, Clicks, ORDERS row A1 addresses in the same column.
    Labels whose row can't be found are skipped with a WARN.
    """
    writes: dict = {}
    for metric_key, sheet_row_label in _META_METRIC_TO_ROW.items():
        value = scraped.get(metric_key)
        if value is not None:
            writes[sheet_row_label] = value

    rows = find_label_rows(
        worksheet, ["SPEND", "Clicks", "ORDERS"], start_row=section_start_row
    )

    col_letter = col_index_to_letter(column_index)
    spend_row = rows.get("SPEND")
    clicks_row = rows.get("Clicks")
    orders_row = rows.get("ORDERS")

    if spend_row and clicks_row:
        writes["cpc"] = f"={col_letter}{spend_row}/{col_letter}{clicks_row}"
    else:
        print("  [WARN] Cannot build 'cpc' formula - SPEND or Clicks row not found.")

    if spend_row:
        writes["DAILY SPEND"] = f"={col_letter}{spend_row}/7"
    else:
        print("  [WARN] Cannot build 'DAILY SPEND' formula - SPEND row not found.")

    if orders_row:
        writes["DAILY ORDERS"] = f"={col_letter}{orders_row}/7"
    else:
        print("  [WARN] Cannot build 'DAILY ORDERS' formula - ORDERS row not found.")

    return writes


def _build_meta_country_writes(
    scraped: dict,
    worksheet,
    column_index: int,
    section_start_row: int,
    section_name: str,
) -> dict:
    """Build {sheet_label: value_or_formula} for a per-country META section.

    SPEND, clicks, ORDERS rows are resolved *within this section* so the
    cpc / ORDERS PER DAY formulas reference this country's own cells.
    `section_name` is only used for log messages.
    """
    writes: dict = {}
    for metric_key, sheet_row_label in _META_COUNTRY_METRIC_TO_ROW.items():
        value = scraped.get(metric_key)
        if value is not None:
            writes[sheet_row_label] = value

    rows = find_label_rows(
        worksheet, ["SPEND", "clicks", "ORDERS"], start_row=section_start_row
    )

    col_letter = col_index_to_letter(column_index)
    spend_row = rows.get("SPEND")
    clicks_row = rows.get("clicks")
    orders_row = rows.get("ORDERS")

    if spend_row and clicks_row:
        writes["cpc"] = f"={col_letter}{spend_row}/{col_letter}{clicks_row}"
    else:
        print(f"  [WARN] {section_name}: cannot build 'cpc' formula - SPEND or clicks row not found.")

    if orders_row:
        writes["ORDERS PER DAY"] = f"={col_letter}{orders_row}/7"
    else:
        print(f"  [WARN] {section_name}: cannot build 'ORDERS PER DAY' formula - ORDERS row not found.")

    return writes


def _build_google_writes(
    scraped: dict,
    worksheet,
    column_index: int,
    section_start_row: int,
) -> dict:
    """Build {sheet_label: value_or_formula} for the GOOGLE (all-countries) section.

    Google Ads spend-derived KPIs only — no Clicks / Impressions / CPC rows.
    DAILY SPEND / DAILY ORDERS formulas mirror the META section's formulas.
    """
    writes: dict = {}
    for metric_key, sheet_row_label in _GOOGLE_METRIC_TO_ROW.items():
        value = scraped.get(metric_key)
        if value is not None:
            writes[sheet_row_label] = value

    rows = find_label_rows(
        worksheet, ["Spend", "ORDERS"], start_row=section_start_row
    )

    col_letter = col_index_to_letter(column_index)
    spend_row = rows.get("Spend")
    orders_row = rows.get("ORDERS")

    if spend_row:
        writes["DAILY SPEND"] = f"={col_letter}{spend_row}/7"
    else:
        print("  [WARN] GOOGLE: cannot build 'DAILY SPEND' formula - Spend row not found.")

    if orders_row:
        writes["DAILY ORDERS"] = f"={col_letter}{orders_row}/7"
    else:
        print("  [WARN] GOOGLE: cannot build 'DAILY ORDERS' formula - ORDERS row not found.")

    return writes


def _build_google_country_writes(
    scraped: dict,
    worksheet,
    column_index: int,
    section_start_row: int,
    section_name: str,
) -> dict:
    """Build {sheet_label: value_or_formula} for a per-country GOOGLE section.

    Resolves Spend, clicks, ORDERS rows within this section so the CPC /
    ORDERS PER DAY formulas reference this country's own cells. These
    sections have no Impressions row.
    """
    writes: dict = {}
    for metric_key, sheet_row_label in _GOOGLE_COUNTRY_METRIC_TO_ROW.items():
        value = scraped.get(metric_key)
        if value is not None:
            writes[sheet_row_label] = value

    rows = find_label_rows(
        worksheet, ["Spend", "clicks", "ORDERS"], start_row=section_start_row
    )

    col_letter = col_index_to_letter(column_index)
    spend_row = rows.get("Spend")
    clicks_row = rows.get("clicks")
    orders_row = rows.get("ORDERS")

    if spend_row and clicks_row:
        writes["CPC"] = f"={col_letter}{spend_row}/{col_letter}{clicks_row}"
    else:
        print(f"  [WARN] {section_name}: cannot build 'CPC' formula - Spend or clicks row not found.")

    if orders_row:
        writes["ORDERS PER DAY"] = f"={col_letter}{orders_row}/7"
    else:
        print(f"  [WARN] {section_name}: cannot build 'ORDERS PER DAY' formula - ORDERS row not found.")

    return writes


def _scrape_google_and_write(
    driver,
    worksheet,
    column_index: int,
    week_header_row: int,
    google_section_label: str,
    is_main: bool,
) -> bool:
    """Switch medium to Google Ads, scrape, write to the given GOOGLE section,
    and flip medium back to Facebook.

    `is_main=True` uses _build_google_writes (GOOGLE all-countries layout);
    otherwise _build_google_country_writes (per-country layout with CPA /
    clicks / CPC / ORDERS PER DAY).

    Returns True on success (including 'nothing written' warning), False on
    fatal errors so the caller can abort the whole run.
    """
    print(f"\n  Switching Medium -> Google Ads for {google_section_label} ...")
    try:
        scraped = extract_weekly_marketing_medium_values(driver, "Google Ads")
    except Exception as exc:
        print(f"  [ERROR] Google scrape for {google_section_label} failed: {exc}")
        import traceback
        traceback.print_exc()
        return False

    for k, v in scraped.items():
        if k == "_raw":
            continue
        print(f"  Scraped  -> {k:<11} = {v!r}")

    section_row = find_section_row(
        worksheet, google_section_label, after_row=week_header_row
    )
    if section_row is None:
        print(f"  [ERROR] Could not find '{google_section_label}' section header in column A.")
        return False

    if is_main:
        writes = _build_google_writes(
            scraped,
            worksheet=worksheet,
            column_index=column_index,
            section_start_row=section_row,
        )
    else:
        writes = _build_google_country_writes(
            scraped,
            worksheet=worksheet,
            column_index=column_index,
            section_start_row=section_row,
            section_name=google_section_label,
        )

    try:
        written = write_weekly_totals(
            worksheet,
            column_index=column_index,
            values=writes,
            week_header_row=section_row,
        )
    except Exception as exc:
        print(f"  [ERROR] Sheet write ({google_section_label}) failed: {exc}")
        import traceback
        traceback.print_exc()
        return False

    if not written:
        print(f"  [WARN] Nothing written - {google_section_label} scrape returned no values.")
    else:
        for label, info in written.items():
            print(f"  [OK] Wrote {label:<14} {info['value']} -> {info['a1']}")

    # Flip medium back so the next country-filter change operates on the
    # Facebook scorecards again.
    try:
        switch_medium(driver, "Facebook")
    except Exception as exc:
        print(f"  [WARN] Failed to switch medium back to Facebook: {exc}")

    return True


def run_smyle_online_strategy_rn_fc1_weekly_report(
    start_date_obj: datetime,
    end_date_obj: datetime,
    start_date_str: str,
    end_date_str: str,
) -> bool:
    """Run the weekly report end-to-end for the given range."""
    print("\n")
    print("=" * 80)
    print("SMYLE_ONLINE_STRATEGY_RN_FC1 (Weekly)".center(80))
    print("=" * 80)
    print(f"\nSelected range: {start_date_str}  ->  {end_date_str}")

    if end_date_obj < start_date_obj:
        print("  [ERROR] End date is before start date.")
        return False

    # ---------------- Phase 1: sheet preparation ----------------
    try:
        worksheet = get_weekly_worksheet()
        prep = find_or_create_week_column(start_date_obj, end_date_obj, worksheet=worksheet)
    except Exception as exc:
        print(f"  [ERROR] Could not prepare WEEKLY sheet: {exc}")
        import traceback
        traceback.print_exc()
        return False

    print(f"\n  Target month      : {prep['month_name']}")
    print(f"  Month header col  : {prep['month_header_col']}")
    zs, ze = prep["zone"]
    print(f"  Weekly zone cols  : {zs}-{ze}  (skips the month-header column)")
    print(f"  Weekly row        : {prep['week_header_row']}")
    print(f"  Week label        : {prep['week_label']}")
    print(f"  Target column     : {prep['column_letter']} (index {prep['column_index']})")
    print(f"  Action            : {prep['action']}")

    # One Chrome session for both scrape phases.
    manager: Optional[BrowserManager] = None
    try:
        manager = BrowserManager(use_existing_chrome=False)
        driver = manager.start_browser()

        # ---------------- Phase 2: totals (KPI + Subscriptions) ----------------
        print("\n[Phase 2] Extracting Net Revenue / Net Rev. FT / Net Rev. RP ...")
        try:
            scraped_totals = extract_weekly_values(
                start_date_obj, end_date_obj, driver=driver
            )
        except Exception as exc:
            print(f"  [ERROR] Looker totals extraction failed: {exc}")
            import traceback
            traceback.print_exc()
            return False

        print(
            f"  Scraped  -> Total={scraped_totals.get('total')!r}, "
            f"Recurring={scraped_totals.get('recurring')!r}, "
            f"New={scraped_totals.get('new')!r}"
        )

        totals_to_write = {
            "Total": scraped_totals.get("total"),
            "Recurring": scraped_totals.get("recurring"),
            "New": scraped_totals.get("new"),
        }

        try:
            written = write_weekly_totals(
                worksheet,
                column_index=prep["column_index"],
                values=totals_to_write,
                week_header_row=prep["week_header_row"],
            )
        except Exception as exc:
            print(f"  [ERROR] Sheet write (totals) failed: {exc}")
            import traceback
            traceback.print_exc()
            return False

        if not written:
            print("  [WARN] Nothing written - all scraped totals were empty.")
        else:
            for label, info in written.items():
                print(f"  [OK] Wrote {label:<10} {info['value']} -> {info['a1']}")

        # ---------------- Phase 3: Marketing Deepdive (Facebook-only) ----------------
        print("\n[Phase 3] Extracting Marketing Deepdive (Facebook-only) ...")
        try:
            scraped_mkt = extract_weekly_marketing_values(
                start_date_obj, end_date_obj, driver=driver
            )
        except Exception as exc:
            print(f"  [ERROR] Looker marketing extraction failed: {exc}")
            import traceback
            traceback.print_exc()
            return False

        for k, v in scraped_mkt.items():
            if k == "_raw":
                continue
            print(f"  Scraped  -> {k:<11} = {v!r}")

        meta_section_row = find_section_row(
            worksheet, "META", after_row=prep["week_header_row"]
        ) or prep["week_header_row"]

        marketing_writes = _build_meta_writes(
            scraped_mkt,
            worksheet=worksheet,
            column_index=prep["column_index"],
            section_start_row=meta_section_row,
        )

        try:
            written_mkt = write_weekly_totals(
                worksheet,
                column_index=prep["column_index"],
                values=marketing_writes,
                week_header_row=meta_section_row,
            )
        except Exception as exc:
            print(f"  [ERROR] Sheet write (META) failed: {exc}")
            import traceback
            traceback.print_exc()
            return False

        if not written_mkt:
            print("  [WARN] Nothing written - META scrape returned no values.")
        else:
            for label, info in written_mkt.items():
                print(f"  [OK] Wrote {label:<13} {info['value']} -> {info['a1']}")

        # Ride-along: swap Medium -> Google Ads with the current (no) country filter,
        # write GOOGLE section, switch back to Facebook.
        if not _scrape_google_and_write(
            driver,
            worksheet,
            column_index=prep["column_index"],
            week_header_row=prep["week_header_row"],
            google_section_label="GOOGLE",
            is_main=True,
        ):
            return False

        # ---------------- Phase 4: Marketing Deepdive (Facebook x Netherlands) ----------------
        print("\n[Phase 4] Narrowing Country -> Netherlands and rescraping for META NL ...")
        try:
            scraped_nl = extract_weekly_marketing_country_values(
                driver, country_name="Netherlands"
            )
        except Exception as exc:
            print(f"  [ERROR] Looker NL extraction failed: {exc}")
            import traceback
            traceback.print_exc()
            return False

        for k, v in scraped_nl.items():
            if k == "_raw":
                continue
            print(f"  Scraped  -> {k:<11} = {v!r}")

        meta_nl_section_row = find_section_row(
            worksheet, "META NL", after_row=prep["week_header_row"]
        )
        if meta_nl_section_row is None:
            print("  [ERROR] Could not find 'META NL' section header in column A.")
            return False

        nl_writes = _build_meta_country_writes(
            scraped_nl,
            worksheet=worksheet,
            column_index=prep["column_index"],
            section_start_row=meta_nl_section_row,
            section_name="META NL",
        )

        try:
            written_nl = write_weekly_totals(
                worksheet,
                column_index=prep["column_index"],
                values=nl_writes,
                week_header_row=meta_nl_section_row,
            )
        except Exception as exc:
            print(f"  [ERROR] Sheet write (META NL) failed: {exc}")
            import traceback
            traceback.print_exc()
            return False

        if not written_nl:
            print("  [WARN] Nothing written - META NL scrape returned no values.")
        else:
            for label, info in written_nl.items():
                print(f"  [OK] Wrote {label:<14} {info['value']} -> {info['a1']}")

        # ---------------- Phase 5: Marketing Deepdive (Facebook x Belgium) ----------------
        print("\n[Phase 5] Narrowing Country -> Belgium and rescraping for META BE ...")
        try:
            scraped_be = extract_weekly_marketing_country_values(
                driver, country_name="Belgium"
            )
        except Exception as exc:
            print(f"  [ERROR] Looker BE extraction failed: {exc}")
            import traceback
            traceback.print_exc()
            return False

        for k, v in scraped_be.items():
            if k == "_raw":
                continue
            print(f"  Scraped  -> {k:<11} = {v!r}")

        meta_be_section_row = find_section_row(
            worksheet, "META BE", after_row=prep["week_header_row"]
        )
        if meta_be_section_row is None:
            print("  [ERROR] Could not find 'META BE' section header in column A.")
            return False

        be_writes = _build_meta_country_writes(
            scraped_be,
            worksheet=worksheet,
            column_index=prep["column_index"],
            section_start_row=meta_be_section_row,
            section_name="META BE",
        )

        try:
            written_be = write_weekly_totals(
                worksheet,
                column_index=prep["column_index"],
                values=be_writes,
                week_header_row=meta_be_section_row,
            )
        except Exception as exc:
            print(f"  [ERROR] Sheet write (META BE) failed: {exc}")
            import traceback
            traceback.print_exc()
            return False

        if not written_be:
            print("  [WARN] Nothing written - META BE scrape returned no values.")
        else:
            for label, info in written_be.items():
                print(f"  [OK] Wrote {label:<14} {info['value']} -> {info['a1']}")

        # ---------------- Phase 5b: Marketing Deepdive (Google Ads x Netherlands+Belgium) ----------------
        # There is no META NL/BE section, so GOOGLE NL/BE gets its own phase:
        # change Country to NL+BE, flip Medium to Google Ads, scrape, write,
        # then flip Medium back to Facebook for Phase 6.
        print(
            "\n[Phase 5b] Country -> Netherlands+Belgium, Medium -> Google Ads for GOOGLE NL/BE ..."
        )
        try:
            scraped_gnlbe = extract_weekly_marketing_country_values(
                driver,
                country_name=["Netherlands", "Belgium"],
                medium="Google Ads",
            )
        except Exception as exc:
            print(f"  [ERROR] Looker GOOGLE NL/BE extraction failed: {exc}")
            import traceback
            traceback.print_exc()
            return False

        for k, v in scraped_gnlbe.items():
            if k == "_raw":
                continue
            print(f"  Scraped  -> {k:<11} = {v!r}")

        google_nlbe_section_row = find_section_row(
            worksheet, "GOOGLE NL/BE", after_row=prep["week_header_row"]
        )
        if google_nlbe_section_row is None:
            print("  [ERROR] Could not find 'GOOGLE NL/BE' section header in column A.")
            return False

        gnlbe_writes = _build_google_country_writes(
            scraped_gnlbe,
            worksheet=worksheet,
            column_index=prep["column_index"],
            section_start_row=google_nlbe_section_row,
            section_name="GOOGLE NL/BE",
        )

        try:
            written_gnlbe = write_weekly_totals(
                worksheet,
                column_index=prep["column_index"],
                values=gnlbe_writes,
                week_header_row=google_nlbe_section_row,
            )
        except Exception as exc:
            print(f"  [ERROR] Sheet write (GOOGLE NL/BE) failed: {exc}")
            import traceback
            traceback.print_exc()
            return False

        if not written_gnlbe:
            print("  [WARN] Nothing written - GOOGLE NL/BE scrape returned no values.")
        else:
            for label, info in written_gnlbe.items():
                print(f"  [OK] Wrote {label:<14} {info['value']} -> {info['a1']}")

        # Flip Medium back to Facebook so Phase 6's country change operates on FB.
        try:
            switch_medium(driver, "Facebook")
        except Exception as exc:
            print(f"  [WARN] Failed to switch medium back to Facebook: {exc}")

        # ---------------- Phase 6: Marketing Deepdive (Facebook x DE+AU+SW) ----------------
        print("\n[Phase 6] Narrowing Country -> Germany+Austria+Switzerland and rescraping for META DE/AU/SW ...")
        try:
            scraped_desw = extract_weekly_marketing_country_values(
                driver, country_name=["Germany", "Austria", "Switzerland"]
            )
        except Exception as exc:
            print(f"  [ERROR] Looker DE/AU/SW extraction failed: {exc}")
            import traceback
            traceback.print_exc()
            return False

        for k, v in scraped_desw.items():
            if k == "_raw":
                continue
            print(f"  Scraped  -> {k:<11} = {v!r}")

        meta_desw_section_row = find_section_row(
            worksheet, "META DE/AU/SW", after_row=prep["week_header_row"]
        )
        if meta_desw_section_row is None:
            print("  [ERROR] Could not find 'META DE/AU/SW' section header in column A.")
            return False

        desw_writes = _build_meta_country_writes(
            scraped_desw,
            worksheet=worksheet,
            column_index=prep["column_index"],
            section_start_row=meta_desw_section_row,
            section_name="META DE/AU/SW",
        )

        try:
            written_desw = write_weekly_totals(
                worksheet,
                column_index=prep["column_index"],
                values=desw_writes,
                week_header_row=meta_desw_section_row,
            )
        except Exception as exc:
            print(f"  [ERROR] Sheet write (META DE/AU/SW) failed: {exc}")
            import traceback
            traceback.print_exc()
            return False

        if not written_desw:
            print("  [WARN] Nothing written - META DE/AU/SW scrape returned no values.")
        else:
            for label, info in written_desw.items():
                print(f"  [OK] Wrote {label:<14} {info['value']} -> {info['a1']}")

        # Ride-along: GOOGLE DE/AU/SW uses the same country filter.
        if not _scrape_google_and_write(
            driver,
            worksheet,
            column_index=prep["column_index"],
            week_header_row=prep["week_header_row"],
            google_section_label="GOOGLE DE/AU/SW",
            is_main=False,
        ):
            return False

        # ---------------- Phase 7: Marketing Deepdive (Facebook x Rest-of-Europe) ----------------
        # REO = all countries EXCEPT the six already covered above.
        reo_excluded = [
            "Netherlands",
            "United Kingdom",
            "Germany",
            "Belgium",
            "Switzerland",
            "Austria",
        ]
        print(
            "\n[Phase 7] Inverting Country -> all EXCEPT "
            + ", ".join(reo_excluded)
            + " for REO ..."
        )
        try:
            scraped_reo = extract_weekly_marketing_country_values(
                driver, country_name=reo_excluded, exclude=True
            )
        except Exception as exc:
            print(f"  [ERROR] Looker REO extraction failed: {exc}")
            import traceback
            traceback.print_exc()
            return False

        for k, v in scraped_reo.items():
            if k == "_raw":
                continue
            print(f"  Scraped  -> {k:<11} = {v!r}")

        reo_section_row = find_section_row(
            worksheet, "REO", after_row=prep["week_header_row"]
        )
        if reo_section_row is None:
            print("  [ERROR] Could not find 'REO' section header in column A.")
            return False

        reo_writes = _build_meta_country_writes(
            scraped_reo,
            worksheet=worksheet,
            column_index=prep["column_index"],
            section_start_row=reo_section_row,
            section_name="REO",
        )

        try:
            written_reo = write_weekly_totals(
                worksheet,
                column_index=prep["column_index"],
                values=reo_writes,
                week_header_row=reo_section_row,
            )
        except Exception as exc:
            print(f"  [ERROR] Sheet write (REO) failed: {exc}")
            import traceback
            traceback.print_exc()
            return False

        if not written_reo:
            print("  [WARN] Nothing written - REO scrape returned no values.")
        else:
            for label, info in written_reo.items():
                print(f"  [OK] Wrote {label:<14} {info['value']} -> {info['a1']}")

        # Ride-along: GOOGLE REO uses the same (inverted) country filter.
        if not _scrape_google_and_write(
            driver,
            worksheet,
            column_index=prep["column_index"],
            week_header_row=prep["week_header_row"],
            google_section_label="GOOGLE REO",
            is_main=False,
        ):
            return False

        # ---------------- Phase 8: Marketing Deepdive (Facebook x United Kingdom) ----------------
        print("\n[Phase 8] Narrowing Country -> United Kingdom and rescraping for META UK ...")
        try:
            scraped_uk = extract_weekly_marketing_country_values(
                driver, country_name="United Kingdom"
            )
        except Exception as exc:
            print(f"  [ERROR] Looker UK extraction failed: {exc}")
            import traceback
            traceback.print_exc()
            return False

        for k, v in scraped_uk.items():
            if k == "_raw":
                continue
            print(f"  Scraped  -> {k:<11} = {v!r}")

        meta_uk_section_row = find_section_row(
            worksheet, "META UK", after_row=prep["week_header_row"]
        )
        if meta_uk_section_row is None:
            print("  [ERROR] Could not find 'META UK' section header in column A.")
            return False

        uk_writes = _build_meta_country_writes(
            scraped_uk,
            worksheet=worksheet,
            column_index=prep["column_index"],
            section_start_row=meta_uk_section_row,
            section_name="META UK",
        )

        try:
            written_uk = write_weekly_totals(
                worksheet,
                column_index=prep["column_index"],
                values=uk_writes,
                week_header_row=meta_uk_section_row,
            )
        except Exception as exc:
            print(f"  [ERROR] Sheet write (META UK) failed: {exc}")
            import traceback
            traceback.print_exc()
            return False

        if not written_uk:
            print("  [WARN] Nothing written - META UK scrape returned no values.")
        else:
            for label, info in written_uk.items():
                print(f"  [OK] Wrote {label:<14} {info['value']} -> {info['a1']}")

        # Ride-along: GOOGLE UK uses the same UK-only country filter.
        if not _scrape_google_and_write(
            driver,
            worksheet,
            column_index=prep["column_index"],
            week_header_row=prep["week_header_row"],
            google_section_label="GOOGLE UK",
            is_main=False,
        ):
            return False

    finally:
        if manager is not None:
            manager.close()

    print("\n[OK] Weekly report done.\n")
    return True


def _parse(date_str: str) -> Optional[datetime]:
    for fmt in ("%d-%b-%Y", "%d-%B-%Y"):
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    return None


def main() -> None:
    print("\nSMYLE_ONLINE_STRATEGY_RN_FC1 (Weekly) - standalone runner")
    start_raw = input("Start date (DD-MMM-YYYY): ").strip()
    end_raw = input("End   date (DD-MMM-YYYY): ").strip()

    start_obj = _parse(start_raw)
    end_obj = _parse(end_raw)
    if not start_obj or not end_obj:
        print("Invalid date format. Use DD-MMM-YYYY (e.g. 13-Apr-2026).")
        return

    run_smyle_online_strategy_rn_fc1_weekly_report(start_obj, end_obj, start_raw, end_raw)


if __name__ == "__main__":
    main()
