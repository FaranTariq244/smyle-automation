"""
Shopify Admin API client for the Smyle store (smyle-bv.myshopify.com).

Authentication: an admin access token (shpat_...) issued to the
"Bridge Influencers" custom app installed on the store. Credentials are
read from (in priority order):
    1. environment variables
    2. shopify.env at the project root (gitignored)
    3. config_store

Required settings:
    SMYLE_SHOP_DOMAIN    e.g. "smyle-bv" (with or without .myshopify.com)
    SMYLE_ACCESS_TOKEN   shpat_...
Optional:
    SHOPIFY_API_VERSION  defaults to 2025-01

Token scopes: read_orders, write_price_rules, read_customers.
Note: read_orders (without read_all_orders) only exposes orders from the
last ~60 days.

API docs: https://shopify.dev/docs/api/admin-graphql
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
ENV_FILE = PROJECT_ROOT / "shopify.env"

_DEFAULT_API_VERSION = "2025-01"

_env_file_cache: Optional[Dict[str, str]] = None


def _load_env_file() -> Dict[str, str]:
    """Parse shopify.env (minimal .env format, supports inline # comments)."""
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


def get_shop_domain() -> str:
    """Return the bare shop subdomain, e.g. "smyle-bv"."""
    domain = _get_config("SMYLE_SHOP_DOMAIN")
    if not domain:
        raise RuntimeError(
            "SMYLE_SHOP_DOMAIN not configured. "
            "Set it in shopify.env at the project root or as an env var."
        )
    return domain.replace(".myshopify.com", "")


def get_access_token() -> str:
    token = _get_config("SMYLE_ACCESS_TOKEN")
    if not token:
        raise RuntimeError(
            "SMYLE_ACCESS_TOKEN not configured. "
            "Set it in shopify.env at the project root or as an env var."
        )
    return token


def _base_url() -> str:
    version = _get_config("SHOPIFY_API_VERSION", _DEFAULT_API_VERSION)
    return f"https://{get_shop_domain()}.myshopify.com/admin/api/{version}"


def _headers() -> Dict[str, str]:
    return {
        "X-Shopify-Access-Token": get_access_token(),
        "Content-Type": "application/json",
    }


def _request_with_retry(method: str, url: str, max_retries: int = 5, **kwargs) -> requests.Response:
    """Send an HTTP request with automatic retry on 429 (rate-limit)."""
    for attempt in range(max_retries + 1):
        resp = requests.request(method, url, **kwargs)
        if resp.status_code != 429:
            return resp

        if attempt == max_retries:
            resp.raise_for_status()

        retry_after = float(resp.headers.get("Retry-After", 2 * (2 ** attempt)))
        log.warning(
            "Shopify 429 on %s %s — retrying in %.1fs (attempt %d/%d)",
            method.upper(), url.split("?")[0], retry_after, attempt + 1, max_retries,
        )
        time.sleep(retry_after)
    return resp  # unreachable, but keeps type checker happy


def graphql(query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Run a GraphQL query against the Admin API and return the `data` payload.

    Raises RuntimeError if the response contains GraphQL errors.
    """
    resp = _request_with_retry(
        "POST",
        f"{_base_url()}/graphql.json",
        json={"query": query, "variables": variables or {}},
        headers=_headers(),
        timeout=60,
    )
    resp.raise_for_status()
    body = resp.json()
    if "errors" in body:
        raise RuntimeError(f"Shopify GraphQL errors: {body['errors']}")
    return body["data"]


def rest_get(endpoint: str, params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """GET a REST Admin API endpoint, e.g. rest_get("shop.json")."""
    resp = _request_with_retry(
        "GET",
        f"{_base_url()}/{endpoint.lstrip('/')}",
        params=params,
        headers=_headers(),
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()


def test_connection() -> Dict[str, Any]:
    """Connectivity check; returns basic shop info (name, domain, currency)."""
    shop = rest_get("shop.json")["shop"]
    log.info("Connected to Shopify shop '%s' (%s)", shop["name"], shop["domain"])
    return shop


_ORDER_FIELDS = """
    id
    name
    createdAt
    sourceName
    tags
    email
    displayFinancialStatus
    displayFulfillmentStatus
    totalPriceSet { shopMoney { amount currencyCode } }
    discountCodes
    customer { firstName lastName email }
    shippingAddress { name city country zip }
    lineItems(first: 25) {
        edges { node { title quantity sku
            discountedUnitPriceSet { shopMoney { amount currencyCode } } } }
    }
"""


def search_orders(query: str, first: int = 25) -> List[Dict[str, Any]]:
    """Search orders with Shopify's order search syntax; returns order nodes.

    Examples: "discount_code:lera15", "name:#SMYLE99680", or free text
    (free text also matches tags).
    """
    data = graphql(
        """
        query($q: String!, $first: Int!) {
            orders(first: $first, query: $q, sortKey: CREATED_AT, reverse: true) {
                edges { node { %s } }
            }
        }
        """ % _ORDER_FIELDS,
        {"q": query, "first": first},
    )
    return [edge["node"] for edge in data["orders"]["edges"]]


def find_order_by_tiktok_id(tiktok_order_id: str) -> Optional[Dict[str, Any]]:
    """Find the Shopify order for an 18-digit TikTok Shop order ID.

    TikTok orders sync into Shopify with a tag "TikTokOrderID:<id>" (the
    sourceIdentifier field is not used), so we search by that tag. Returns
    the order node, or None if it never synced or is outside the ~60-day
    window the token can see.
    """
    orders = search_orders(f"tag:'TikTokOrderID:{tiktok_order_id}'", first=5)
    if not orders:
        log.warning("No Shopify order found for TikTok order %s", tiktok_order_id)
        return None
    if len(orders) > 1:
        log.warning(
            "Multiple Shopify orders matched TikTok order %s: %s",
            tiktok_order_id, [o["name"] for o in orders],
        )
    return orders[0]


def find_orders_by_discount_code(code: str, first: int = 25) -> List[Dict[str, Any]]:
    """List orders that used a given discount code (e.g. influencer codes)."""
    return search_orders(f"discount_code:{code}", first=first)
