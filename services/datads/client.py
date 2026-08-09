"""
DatAds public API client (https://public.api.datads.io).

Replaces the Selenium/UI scraping path for DataAds reports. Credentials are
read from (in priority order):
    1. environment variables
    2. datads.env at the project root (gitignored)
    3. config_store

Required settings:
    DATADS_API_KEY       datads_live_v1_...
Optional:
    DATADS_REPORT_ID     UUID of the saved Top Performing report to mirror.
                         Defaults to the "Landing Page Comparison" report that
                         the UI automation used.

Rate limit: 10 requests/minute per endpoint. A normal report run costs 2
requests (config + breakdown), so the limit only matters when backfilling
many dates in a loop — hence the 429 retry below.

API docs: https://public.api.datads.io/docs
"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

from config_store import get_setting

log = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = PROJECT_ROOT / "datads.env"

BASE_URL = "https://public.api.datads.io"

# The saved report the UI automation pointed at:
# https://app.datads.io/creative-reporting/detail/<uuid>
DEFAULT_REPORT_ID = "c9f95694-cfd5-4255-8414-501e5fb11369"

_env_file_cache: Optional[Dict[str, str]] = None


def _load_env_file() -> Dict[str, str]:
    """Parse datads.env (minimal .env format, supports inline # comments)."""
    global _env_file_cache
    if _env_file_cache is None:
        env: Dict[str, str] = {}
        if ENV_FILE.exists():
            for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, val = line.split("=", 1)
                env[key.strip()] = val.split("#", 1)[0].strip()
        _env_file_cache = env
    return _env_file_cache


def _get_config(key: str, default: Optional[str] = None) -> Optional[str]:
    return os.getenv(key) or _load_env_file().get(key) or get_setting(key, default)


def get_api_key() -> str:
    key = _get_config("DATADS_API_KEY")
    if not key:
        raise RuntimeError(
            "DATADS_API_KEY not configured. Set it in datads.env at the project "
            "root, as an environment variable, or via the web app settings."
        )
    return key


def get_report_id() -> str:
    return _get_config("DATADS_REPORT_ID", DEFAULT_REPORT_ID) or DEFAULT_REPORT_ID


def _headers() -> Dict[str, str]:
    return {"X-API-Key": get_api_key(), "Content-Type": "application/json"}


def _request(method: str, path: str, max_retries: int = 4, **kwargs) -> Any:
    """Send a request, retrying on 429 (rate limit) and 5xx with backoff."""
    url = f"{BASE_URL}{path}"
    delay = 8.0  # the limit is per-minute, so back off in meaningful steps
    last_error = None

    for attempt in range(max_retries):
        try:
            resp = requests.request(method, url, headers=_headers(), timeout=90, **kwargs)
        except requests.RequestException as exc:
            last_error = exc
            log.warning("DatAds %s %s failed (%s), retrying...", method, path, exc)
            time.sleep(delay)
            delay *= 2
            continue

        if resp.status_code == 429 or resp.status_code >= 500:
            last_error = RuntimeError(f"HTTP {resp.status_code}: {resp.text[:300]}")
            wait = float(resp.headers.get("Retry-After") or delay)
            log.warning("DatAds %s %s -> HTTP %s, retrying in %.0fs",
                        method, path, resp.status_code, wait)
            time.sleep(wait)
            delay *= 2
            continue

        if not resp.ok:
            raise RuntimeError(
                f"DatAds API {method} {path} failed: HTTP {resp.status_code} — {resp.text[:500]}"
            )
        return resp.json()

    raise RuntimeError(f"DatAds API {method} {path} failed after {max_retries} attempts: {last_error}")


def get_report_config(report_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Fetch a saved Top Performing report's configuration.

    We read provider / groupBy / globalFilter from the live report rather than
    hard-coding them, so edits made in the DataAds UI (a changed spend filter,
    a different grouping) still flow through to the automation — exactly as they
    did when we were scraping the rendered page.

    Note the report's own startDate/endDate are deliberately ignored: this
    endpoint has no date parameter, so dates come from the caller instead and
    are applied via `breakdown()`.
    """
    report_id = report_id or get_report_id()
    payload = _request("GET", f"/v1/reports/top-performing/{report_id}"
                              f"?pageNumber=1&pageSize=10")
    return payload["data"]


def breakdown(provider: str, metrics: List[str], start_date: str, end_date: str,
              group_by: str = "LANDING_PAGE",
              filter_: Optional[List] = None,
              page_size: int = 100) -> List[Dict[str, Any]]:
    """
    Aggregate metrics by dimension over an explicit date range (max 90 days).

    Returns the full list of groups, following pagination. Each entry is
    {displayName, breakdown, count, adIds, metrics: {name: number}}.
    """
    rows: List[Dict[str, Any]] = []
    page = 1
    while True:
        body = {
            "provider": provider,
            "metrics": metrics,
            "startDate": start_date,
            "endDate": end_date,
            "groupBy": group_by,
            "pageNumber": page,
            "pageSize": page_size,
        }
        if filter_:
            body["filter"] = filter_

        payload = _request("POST", "/v1/insights/breakdown", json=body)
        rows.extend(payload.get("data", []))

        pagination = payload.get("pagination") or {}
        if page >= int(pagination.get("totalPages") or 1):
            break
        page += 1

    return rows
