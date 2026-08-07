"""
Google Sheets export for order lookups (order_lookup.py).

Writes a flat table of orders — Shopify status, fulfilment status, tracking,
and the my-fulfilment.com side — into a tab of its own.

Setting key: ORDER_EXPORT_SHEET_URL
    Full Google Sheets URL. Set it in Settings in the web app, or in config.db.
    Falls back to DATADS_SHEET_URL only if you point it there explicitly — the
    export deliberately does not borrow another report's sheet by default.

Each run targets one worksheet tab and REPLACES its contents, so re-running for
the same range is idempotent rather than appending duplicates.
"""

from __future__ import annotations

import json
import logging
import os
import re
from typing import List, Optional

import gspread
from google.oauth2.service_account import Credentials

from config_store import get_setting

log = logging.getLogger(__name__)

SETTING_KEY = "ORDER_EXPORT_SHEET_URL"

SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Column order in the sheet. Keys not present on a row become "".
# Kept explicit rather than derived from the dict so the sheet layout stays
# stable even when the lookup gains new fields.
COLUMNS = [
    "order", "created", "source", "financial", "fulfillment", "cancelled",
    "has_tracking", "tracking", "carrier",
    "portal_status", "portal_tracking", "shipper", "delivery_mode",
    "in_wms", "wms_ref", "portal_error",
    "country", "city", "customer", "email",
    "total", "currency", "units", "items",
]


def _client() -> gspread.Client:
    env_json = os.getenv("GOOGLE_SHEETS_CREDENTIALS")
    if env_json:
        creds = Credentials.from_service_account_info(json.loads(env_json), scopes=SCOPES)
    else:
        creds = Credentials.from_service_account_file("auth.json", scopes=SCOPES)
    return gspread.authorize(creds)


def get_spreadsheet(sheet_url: Optional[str] = None):
    """Open the order-export spreadsheet."""
    sheet_url = sheet_url or get_setting(SETTING_KEY)
    if not sheet_url:
        raise ValueError(
            f"{SETTING_KEY} is not set. Add the Google Sheets URL via Settings "
            "in the web app, or pass --sheet-url."
        )
    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", sheet_url)
    if not match:
        raise ValueError(f"Could not extract spreadsheet ID from URL: {sheet_url}")
    return _client().open_by_key(match.group(1))


def export_orders(rows: List[dict], tab: str, sheet_url: Optional[str] = None) -> str:
    """Write rows to a worksheet tab, replacing whatever was there.

    Returns the URL of the tab that was written.
    """
    if not rows:
        raise ValueError("Nothing to export — the row list is empty.")

    spreadsheet = get_spreadsheet(sheet_url)

    # gspread caps a tab title at 100 chars and rejects a few characters.
    tab = re.sub(r"[\[\]:*?/\\]", "-", tab)[:99]

    try:
        worksheet = spreadsheet.worksheet(tab)
        worksheet.clear()
        log.info("Replacing existing tab %r", tab)
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(
            title=tab, rows=max(len(rows) + 10, 100), cols=len(COLUMNS))
        log.info("Created tab %r", tab)

    header = [c.replace("_", " ").title() for c in COLUMNS]
    body = [[str(r.get(c, "") if r.get(c) is not None else "") for c in COLUMNS]
            for r in rows]

    # One batched write — a per-row update would burn through the Sheets API
    # quota on a 471-order day.
    worksheet.update(values=[header] + body, range_name="A1")
    worksheet.freeze(rows=1)
    worksheet.format("A1:Z1", {"textFormat": {"bold": True}})

    log.info("Wrote %d row(s) to tab %r", len(rows), tab)
    return f"{spreadsheet.url}#gid={worksheet.id}"
