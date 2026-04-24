"""
BigQuery client with service-account auth (never expires).

Authentication priority:
    1. Service account key file (bigquery_sa.json) — preferred, never expires.
    2. OAuth user credentials (bigquery_auth.json) — fallback, may expire.

Configuration:
    - BIGQUERY_BILLING_PROJECT : GCP project used to run (and bill) queries.
    - BIGQUERY_DATA_PROJECT   : GCP project that owns the data tables.
    - BIGQUERY_DATASET        : Default dataset to query.

All three can be overridden via environment variables or config_store settings.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional

from google.auth.transport.requests import Request
from google.cloud import bigquery
from google.oauth2.credentials import Credentials
from google.oauth2.service_account import Credentials as ServiceAccountCredentials

from config_store import get_setting

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
SA_CREDENTIALS_PATH = PROJECT_ROOT / "bigquery_sa.json"
CREDENTIALS_PATH = PROJECT_ROOT / "bigquery_auth.json"

# Google's public OAuth client (same one used by gcloud CLI)
_OAUTH_CLIENT_ID = (
    "764086051850-6qr4p6gpi6hn506pt8ejuq83di341hur.apps.googleusercontent.com"
)
_OAUTH_CLIENT_SECRET = "d-FL95Q19q7MQmFpd7hHD0Ty"

_SCOPES = [
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/cloud-platform",
]

# Defaults — can be overridden via env vars or config_store
_DEFAULT_BILLING_PROJECT = "gen-lang-client-0136431006"
_DEFAULT_DATA_PROJECT = "pelagic-core-307421"
_DEFAULT_DATASET = "smyle_dbt_prod"


def _get_config(key: str, default: str) -> str:
    """Read a value from env → config_store → default."""
    return os.getenv(key) or get_setting(key) or default


def get_billing_project() -> str:
    return _get_config("BIGQUERY_BILLING_PROJECT", _DEFAULT_BILLING_PROJECT)


def get_data_project() -> str:
    return _get_config("BIGQUERY_DATA_PROJECT", _DEFAULT_DATA_PROJECT)


def get_dataset() -> str:
    return _get_config("BIGQUERY_DATASET", _DEFAULT_DATASET)


_sa_logged = False


def _load_service_account() -> Optional[ServiceAccountCredentials]:
    """Load service account credentials from bigquery_sa.json (never expires)."""
    global _sa_logged
    if not SA_CREDENTIALS_PATH.exists():
        return None
    try:
        creds = ServiceAccountCredentials.from_service_account_file(
            str(SA_CREDENTIALS_PATH), scopes=_SCOPES
        )
        if not _sa_logged:
            print("[OK] BigQuery: using service account (never expires)")
            _sa_logged = True
        return creds
    except Exception as exc:
        print(f"[WARN] Could not load service account: {exc}")
        return None


def _load_credentials() -> Optional[Credentials]:
    """Load saved OAuth credentials from disk."""
    if not CREDENTIALS_PATH.exists():
        return None
    with open(CREDENTIALS_PATH) as f:
        data = json.load(f)
    creds = Credentials(
        token=data.get("token"),
        refresh_token=data.get("refresh_token"),
        token_uri=data.get("token_uri"),
        client_id=data.get("client_id"),
        client_secret=data.get("client_secret"),
        scopes=data.get("scopes"),
    )
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        _save_credentials(creds)
    return creds


def _save_credentials(creds: Credentials) -> None:
    """Persist OAuth credentials to disk."""
    data = {
        "token": creds.token,
        "refresh_token": creds.refresh_token,
        "token_uri": creds.token_uri,
        "client_id": creds.client_id,
        "client_secret": creds.client_secret,
        "scopes": list(creds.scopes) if creds.scopes else _SCOPES,
    }
    with open(CREDENTIALS_PATH, "w") as f:
        json.dump(data, f, indent=2)


def authenticate() -> Credentials:
    """
    Full OAuth flow — opens browser for Google login.
    Call this once to set up credentials, or when refresh token expires.
    """
    from google_auth_oauthlib.flow import InstalledAppFlow

    client_config = {
        "installed": {
            "client_id": _OAUTH_CLIENT_ID,
            "client_secret": _OAUTH_CLIENT_SECRET,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": ["http://localhost"],
        }
    }
    flow = InstalledAppFlow.from_client_config(client_config, scopes=_SCOPES)
    creds = flow.run_local_server(port=8085, open_browser=True)
    _save_credentials(creds)
    print("[OK] BigQuery credentials saved to bigquery_auth.json")
    return creds


def get_credentials():
    """
    Return valid credentials. Tries service account first (never expires),
    falls back to OAuth user credentials.
    """
    # 1. Service account — preferred, never expires
    sa_creds = _load_service_account()
    if sa_creds is not None:
        return sa_creds

    # 2. OAuth user credentials — fallback
    creds = _load_credentials()
    if creds and creds.valid:
        return creds
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
        _save_credentials(creds)
        return creds
    return authenticate()


def get_bigquery_client() -> bigquery.Client:
    """Return an authenticated BigQuery client ready to run queries."""
    creds = get_credentials()
    return bigquery.Client(project=get_billing_project(), credentials=creds)


def run_query(sql: str, **kwargs) -> list[dict]:
    """
    Execute a SQL query and return results as a list of dicts.

    The query can reference tables as:
        `{project}.{dataset}.{table}`
    or use the helper `fqn()` to build fully-qualified table names.

    Args:
        sql:    BigQuery Standard SQL query string.
        **kwargs: Passed to bigquery.Client.query().

    Returns:
        List of row dicts.
    """
    client = get_bigquery_client()
    results = client.query(sql, **kwargs)
    return [dict(row) for row in results]


def fqn(table: str) -> str:
    """
    Return the fully-qualified table name for a table in the Smyle dataset.

    Example:
        fqn("ads_spend")
        → "`pelagic-core-307421.smyle_dbt_prod.ads_spend`"
    """
    return f"`{get_data_project()}.{get_dataset()}.{table}`"
