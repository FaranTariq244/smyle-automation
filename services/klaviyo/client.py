"""
Klaviyo API client for email marketing metrics.

Authentication: uses a private API key stored in config_store (KLAVIYO_API_KEY).
The key is set once via the web UI Settings > API Keys tab.

API docs: https://developers.klaviyo.com/en/reference/api_overview
"""

from __future__ import annotations

import os
from typing import Any, Dict, Optional

import requests

from config_store import get_setting

_BASE_URL = "https://a.klaviyo.com/api"
_API_REVISION = "2025-04-15"


def get_api_key() -> str:
    """Return the Klaviyo private API key from env or config_store."""
    key = os.getenv("KLAVIYO_API_KEY") or get_setting("KLAVIYO_API_KEY")
    if not key:
        raise RuntimeError(
            "KLAVIYO_API_KEY not configured. "
            "Set it via Settings > API Keys in the web UI."
        )
    return key


def _headers() -> Dict[str, str]:
    return {
        "Authorization": f"Klaviyo-API-Key {get_api_key()}",
        "Accept": "application/vnd.api+json",
        "Content-Type": "application/json",
        "revision": _API_REVISION,
    }


def _post(endpoint: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """POST to a Klaviyo API endpoint and return the JSON response."""
    url = f"{_BASE_URL}/{endpoint}"
    resp = requests.post(url, json=payload, headers=_headers(), timeout=60)
    resp.raise_for_status()
    return resp.json()


def _get(endpoint: str, params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """GET from a Klaviyo API endpoint and return the JSON response."""
    url = f"{_BASE_URL}/{endpoint}"
    resp = requests.get(url, params=params, headers=_headers(), timeout=60)
    resp.raise_for_status()
    return resp.json()
