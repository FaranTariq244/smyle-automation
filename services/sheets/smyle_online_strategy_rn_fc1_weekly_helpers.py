"""
Helper functions for the WEEKLY report sheet.

Given a user-selected date range (start + end), this module locates the correct
column on the WEEKLY tab:
  - Figures out which MONTH the week belongs to (the one with more days in range).
  - Finds that month's header cell in row 3 (e.g. "APR").
  - Scans row 24 inside that month's zone for an existing week label.
  - If missing, writes a new week label into the next empty column of that zone.

No data is written to other rows here - this is the sheet-preparation step.
"""

from __future__ import annotations

import os
import json
import re
from calendar import monthrange
from datetime import datetime, timedelta
from typing import Optional, Tuple, List

import gspread
from google.oauth2.service_account import Credentials

from config_store import get_setting


SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Rows on the WEEKLY tab (1-based).
MONTH_HEADER_ROW = 3        # row 3 holds month names: APR, MAY, JUN ...
# The weekly-header row is discovered at runtime by scanning column A for
# a cell whose text is exactly "Weekly" (case-insensitive).
WEEK_HEADER_LABEL = "weekly"

MONTH_ABBR = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
MONTH_FULL = ["", "January", "February", "March", "April", "May", "June",
              "July", "August", "September", "October", "November", "December"]


def _gspread_client() -> gspread.Client:
    env_json = os.getenv("GOOGLE_SHEETS_CREDENTIALS")
    if env_json:
        info = json.loads(env_json)
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    else:
        creds = Credentials.from_service_account_file("auth.json", scopes=SCOPES)
    return gspread.authorize(creds)


SETTING_KEY = "SMYLE_ONLINE_STRATEGY_RN_FC1_WEEKLY_SHEET_URL"


def get_weekly_worksheet() -> gspread.Worksheet:
    """Open the WEEKLY worksheet using the configured sheet URL."""
    sheet_url = get_setting(SETTING_KEY)
    if not sheet_url:
        raise ValueError(
            f"{SETTING_KEY} is not set. "
            "Add it via the web UI settings or config.db."
        )

    ss_match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", sheet_url)
    if not ss_match:
        raise ValueError(f"Could not extract spreadsheet ID from URL: {sheet_url}")

    gc = _gspread_client()
    ss = gc.open_by_key(ss_match.group(1))

    # Prefer gid if present in the URL, else fall back to the tab named "WEEKLY".
    gid_match = re.search(r"[?#&]gid=(\d+)", sheet_url)
    if gid_match:
        return ss.get_worksheet_by_id(int(gid_match.group(1)))

    for ws in ss.worksheets():
        if ws.title.strip().lower() == "weekly":
            return ws
    raise ValueError("No 'WEEKLY' tab found and no gid in URL.")


# ---------------------------------------------------------------------------
# Date / label utilities
# ---------------------------------------------------------------------------

def pick_target_month(start_date: datetime, end_date: datetime) -> Tuple[int, int]:
    """Return (year, month) the week should be filed under.

    Rule: if the range overlaps two months, pick the one with MORE days.
    Ties go to the end month (so a Mon-Sun week straddling 2-5 days each way
    lands where the 'final' weekend is).
    """
    counts: dict[Tuple[int, int], int] = {}
    cur = start_date
    while cur.date() <= end_date.date():
        key = (cur.year, cur.month)
        counts[key] = counts.get(key, 0) + 1
        cur += timedelta(days=1)

    # Sort by day-count desc, then by (year, month) asc so later month wins ties.
    ranked = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
    # If top two are tied on count, prefer the later month.
    if len(ranked) >= 2 and ranked[0][1] == ranked[1][1]:
        return max(ranked[0][0], ranked[1][0])
    return ranked[0][0]


def format_week_label(start_date: datetime, end_date: datetime) -> str:
    """Format the week label to match the user's sheet conventions.

    Examples:
      - same month:   'wk 15 6-12 April'      (full month name)
      - cross-month:  'wk 14 30 Mar - 5 Apr'  (abbreviated month)
    """
    iso_week = start_date.isocalendar()[1]
    if start_date.month == end_date.month:
        month_full = MONTH_FULL[start_date.month]
        return f"wk {iso_week} {start_date.day}-{end_date.day} {month_full}"
    s_mon = MONTH_ABBR[start_date.month]
    e_mon = MONTH_ABBR[end_date.month]
    return f"wk {iso_week} {start_date.day} {s_mon} - {end_date.day} {e_mon}"


def _normalize(s: str) -> str:
    """Lowercase, collapse whitespace — for lenient label matching."""
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def _week_label_matches(existing: str, candidate: str, iso_week: int) -> bool:
    """Soft-match: exact (normalized) OR both reference the same 'wk N'."""
    if not existing:
        return False
    if _normalize(existing) == _normalize(candidate):
        return True
    # Fallback: accept any row-24 cell that mentions the same week number.
    m = re.search(r"w[k]?\s*(\d+)", existing.lower())
    return bool(m and int(m.group(1)) == iso_week)


# ---------------------------------------------------------------------------
# Column math
# ---------------------------------------------------------------------------

def col_index_to_letter(col_idx: int) -> str:
    """1-based column index to A1 letter (1->A, 27->AA)."""
    result = ""
    while col_idx > 0:
        col_idx -= 1
        result = chr(65 + (col_idx % 26)) + result
        col_idx //= 26
    return result


def _find_month_zone(month_row: List[str], month_idx: int) -> Tuple[int, int, int]:
    """Return (header_col, zone_start, zone_end) - all 1-based, inclusive.

    - header_col is the column of the month label itself (e.g. APR).
    - zone_start is the first column AFTER the month label (weeks start here;
      the month-header column itself is never used for weeks).
    - zone_end is the column just before the next month header, or the end
      of the row if none.
    """
    target_abbr = MONTH_ABBR[month_idx].lower()
    target_full = MONTH_FULL[month_idx].lower()

    header_col = None
    for i, cell in enumerate(month_row, start=1):
        val = (cell or "").strip().lower()
        if val in (target_abbr, target_full):
            header_col = i
            break

    if header_col is None:
        raise ValueError(
            f"Month '{MONTH_ABBR[month_idx]}' not found in row {MONTH_HEADER_ROW} "
            f"of the WEEKLY tab."
        )

    # Walk forward to find the next month header.
    known_months = set(
        [m.lower() for m in MONTH_ABBR[1:]] + [m.lower() for m in MONTH_FULL[1:]]
    )
    end = len(month_row)  # default: to end of row
    for i in range(header_col + 1, len(month_row) + 1):
        val = (month_row[i - 1] or "").strip().lower()
        if val and val != target_abbr and val != target_full and val in known_months:
            end = i - 1
            break

    zone_start = header_col + 1  # skip the month label column itself
    return header_col, zone_start, end


def _find_week_header_row(worksheet) -> int:
    """Scan column A for a cell whose value is 'Weekly' and return its row.

    We look up to row 500 which is more than enough for the sheets we've
    seen. Raises if not found.
    """
    col_a = worksheet.col_values(1)[:500]
    for i, cell in enumerate(col_a, start=1):
        if (cell or "").strip().lower() == WEEK_HEADER_LABEL:
            return i
    raise ValueError(
        "Could not locate the 'Weekly' header row - no cell in column A "
        "contains the text 'Weekly'."
    )


# ---------------------------------------------------------------------------
# Row-label lookup + value writes (Phase 2)
# ---------------------------------------------------------------------------

def find_section_row(
    worksheet,
    section_label: str,
    after_row: int,
    search_span: int = 200,
) -> Optional[int]:
    """Return the 1-based row whose column-A cell matches section_label.

    Searches col A starting at `after_row + 1` for up to `search_span` rows.
    Match is case-insensitive and whitespace-normalized. Returns None if
    no match is found.
    """
    col_a = worksheet.col_values(1)
    upper = min(len(col_a), after_row + search_span)
    wanted = _normalize(section_label)
    for i in range(after_row + 1, upper + 1):
        cell_val = col_a[i - 1] if i - 1 < len(col_a) else ""
        if _normalize(cell_val) == wanted:
            return i
    return None


def find_label_rows(
    worksheet,
    labels: List[str],
    start_row: int,
    search_span: int = 40,
) -> dict:
    """Locate rows in column A matching given labels (case-insensitive).

    Search starts at `start_row + 1` and covers the next `search_span` rows.
    Returns a dict {label: row_number}. Labels not found are omitted.
    """
    col_a = worksheet.col_values(1)
    upper = min(len(col_a), start_row + search_span)
    wanted = {lab.strip().lower(): lab for lab in labels}
    found: dict = {}
    for i in range(start_row + 1, upper + 1):
        cell_val = (col_a[i - 1] if i - 1 < len(col_a) else "").strip().lower()
        if cell_val in wanted and wanted[cell_val] not in found:
            found[wanted[cell_val]] = i
            if len(found) == len(wanted):
                break
    return found


def write_weekly_totals(
    worksheet,
    column_index: int,
    values: dict,
    week_header_row: Optional[int] = None,
) -> dict:
    """Write Total / Recurring / New values into the target week column.

    Args:
        worksheet: gspread worksheet for the WEEKLY tab.
        column_index: 1-based column to write into (from Phase 1).
        values: dict with optional keys 'Total', 'Recurring', 'New' -> numbers.
                Keys with value None are skipped (nothing written).
        week_header_row: optional, pass through to avoid an extra lookup.

    Returns a dict describing what was written: {label: {'row': r, 'value': v}}.
    """
    if week_header_row is None:
        week_header_row = _find_week_header_row(worksheet)

    labels = [k for k, v in values.items() if v is not None]
    if not labels:
        return {}

    rows = find_label_rows(worksheet, labels, start_row=week_header_row)

    written: dict = {}
    a1_updates = []
    for label in labels:
        row = rows.get(label)
        if row is None:
            print(f"  [WARN] Row label '{label}' not found below 'Weekly' row; skipping.")
            continue
        a1 = f"{col_index_to_letter(column_index)}{row}"
        a1_updates.append({"range": a1, "values": [[values[label]]]})
        written[label] = {"row": row, "column_index": column_index, "value": values[label], "a1": a1}

    if a1_updates:
        worksheet.batch_update(
            [{"range": u["range"], "values": u["values"]} for u in a1_updates],
            value_input_option="USER_ENTERED",
        )

    return written


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def find_or_create_week_column(
    start_date: datetime,
    end_date: datetime,
    worksheet: Optional[gspread.Worksheet] = None,
    dry_run: bool = False,
) -> dict:
    """Find (or create) the target column for this week on the WEEKLY tab.

    Args:
        start_date, end_date: inclusive week range chosen by the user.
        worksheet: optional pre-opened worksheet (tests pass one in).
        dry_run: if True, do not write anything - only report what would happen.

    Returns a dict with:
        action: 'found' | 'created' | 'would_create'
        column_letter, column_index
        week_label
        month_name
        zone: (start_col, end_col) 1-based
    """
    if worksheet is None:
        worksheet = get_weekly_worksheet()

    year, month = pick_target_month(start_date, end_date)
    label = format_week_label(start_date, end_date)
    iso_week = start_date.isocalendar()[1]

    # Locate the weekly header row dynamically (column A cell 'Weekly').
    week_header_row = _find_week_header_row(worksheet)

    # Read the two relevant rows once.
    month_row = worksheet.row_values(MONTH_HEADER_ROW)
    week_row = worksheet.row_values(week_header_row)

    header_col, zone_start, zone_end = _find_month_zone(month_row, month)

    # Pad week_row so index math stays simple.
    if len(week_row) < zone_end:
        week_row = week_row + [""] * (zone_end - len(week_row))

    # Look for an existing match inside the zone.
    for col in range(zone_start, zone_end + 1):
        existing = week_row[col - 1] if col - 1 < len(week_row) else ""
        if _week_label_matches(existing, label, iso_week):
            return {
                "action": "found",
                "column_letter": col_index_to_letter(col),
                "column_index": col,
                "week_label": existing,
                "week_header_row": week_header_row,
                "month_header_col": col_index_to_letter(header_col),
                "month_name": MONTH_FULL[month],
                "zone": (zone_start, zone_end),
            }

    # Not found - decide where to write the new label:
    #   1. If the target month's zone still has an empty column, use the
    #      first empty column after the last filled week inside that zone.
    #   2. Otherwise, overflow: append right after the LAST filled cell in
    #      the weekly row (globally). This matches the user's rule:
    #      "create a new line in front of last and add selected date".
    last_filled_in_zone = None
    for col in range(zone_start, zone_end + 1):
        val = week_row[col - 1] if col - 1 < len(week_row) else ""
        if val.strip():
            last_filled_in_zone = col

    zone_has_space = (
        last_filled_in_zone is None or last_filled_in_zone < zone_end
    )

    if zone_has_space:
        target_col = (
            last_filled_in_zone + 1 if last_filled_in_zone is not None else zone_start
        )
    else:
        # Zone full - append after the globally-last filled column in row.
        global_last_filled = 0
        for col in range(1, len(week_row) + 1):
            if (week_row[col - 1] or "").strip():
                global_last_filled = col
        target_col = max(zone_end + 1, global_last_filled + 1)

    action = "would_create" if dry_run else "created"
    if not dry_run:
        # Just write the week-label value and bold it. Columns themselves are
        # pre-created manually in the sheet with their own formatting/bands.
        worksheet.update_cell(week_header_row, target_col, label)
        a1 = f"{col_index_to_letter(target_col)}{week_header_row}"
        try:
            worksheet.format(a1, {"textFormat": {"bold": True}})
        except Exception as fmt_err:
            print(f"  [WARN] Could not bold {a1}: {fmt_err}")

    return {
        "action": action,
        "column_letter": col_index_to_letter(target_col),
        "column_index": target_col,
        "week_label": label,
        "week_header_row": week_header_row,
        "month_header_col": col_index_to_letter(header_col),
        "month_name": MONTH_FULL[month],
        "zone": (zone_start, zone_end),
    }
