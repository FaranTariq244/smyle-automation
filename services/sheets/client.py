"""
Google Sheets client using service account OAuth flow.
Handles authentication and worksheet access.
"""

import json
import gspread
from google.oauth2.service_account import Credentials
from config_store import get_setting

SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]


def get_credentials():
    """
    Get Google service account credentials.

    Tries to load credentials from:
    1. GOOGLE_SHEETS_CREDENTIALS environment variable (raw JSON string)
    2. auth.json file in project root

    Returns:
        Credentials: Google service account credentials with required scopes
    """
    env_json = os.getenv("GOOGLE_SHEETS_CREDENTIALS")
    if env_json:
        info = json.loads(env_json)
        return Credentials.from_service_account_info(info, scopes=SCOPES)
    return Credentials.from_service_account_file("auth.json", scopes=SCOPES)


def get_worksheet():
    """
    Get the Google Sheets worksheet to work with.

    Uses locally stored settings (config.db):
    - SPREAD_SHEET_NAME: Name of the spreadsheet
    - WORK_SHEET_NAME: Name of the worksheet within the spreadsheet

    Returns:
        gspread.Worksheet: The worksheet object to read/write data

    Raises:
        AssertionError: If SPREAD_SHEET_NAME or WORK_SHEET_NAME are not set
    """
    creds = get_credentials()
    gc = gspread.authorize(creds)
    ss_name = get_setting("SPREAD_SHEET_NAME")
    ws_name = get_setting("WORK_SHEET_NAME")
    assert ss_name and ws_name, "Missing SPREAD_SHEET_NAME or WORK_SHEET_NAME."
    ss = gc.open(ss_name)
    return ss.worksheet(ws_name)
