"""
Shopify fulfilment writes — creating fulfilments and attaching tracking.

Separate from client.py on purpose: client.py is read-only and is used by
existing live workflows. Everything that WRITES to Shopify lives here, so the
new tracking sync can be tested without touching proven code.

Requires these token scopes (granted 2026-08-07):
    write_merchant_managed_fulfillment_orders   create fulfilments at own locations
    write_fulfillments                          edit tracking on existing fulfilments

Reference shape, taken from real orders on this store:
    #SMYLE77964   PostNL Domestic       LA036232528NL      (carrier + number + url)
    #SMYLE119086  <none>                3SOSVJ0630409      (number only, no url)
Both were created with tracking supplied in the SAME call that created the
fulfilment (createdAt == updatedAt), which is the pattern followed here.

API docs: https://shopify.dev/docs/api/admin-graphql/latest/mutations/fulfillmentCreate
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from . import audit, client

log = logging.getLogger(__name__)


def _order_name(fulfillment_name: str) -> str:
    """"#SMYLE120024-F1" -> "#SMYLE120024", so the log is searchable by order."""
    return fulfillment_name.rsplit("-F", 1)[0] if fulfillment_name else ""

# Carrier strings Shopify recognises well enough to auto-generate a tracking
# URL. Pass one of these as `company` and you can leave `url` empty.
# GOTCHA: the string must match exactly — "PostNL" alone does NOT resolve, and
# Domestic vs International are distinct entries.
KNOWN_CARRIERS = {
    "PostNL Domestic",
    "PostNL International",
    "DHL Express",
    "DHL eCommerce",
    "DPD",
    "GLS",
    "UPS",
    "FedEx",
    "Royal Mail",
    "bpost",
    "Bring",
    "Colissimo",
}

# Maps the "Shipper" column in my-fulfilment.com onto a Shopify carrier string.
# The portal reports a bare shipper name; Shopify wants the domestic/
# international variant, which depends on the destination country.
_SHIPPER_MAP = {
    "postnl": ("PostNL Domestic", "PostNL International"),
    "dhl": ("DHL eCommerce", "DHL Express"),
    "dpd": ("DPD", "DPD"),
    "gls": ("GLS", "GLS"),
    "ups": ("UPS", "UPS"),
}

DOMESTIC_COUNTRY = "NL"

# Shopify treats any `company` string it doesn't recognise as a custom carrier
# — "Other" is the conventional label, matching the admin UI's dropdown.
OTHER_CARRIER = "Other"


def normalize_tracking_url(url: Optional[str]) -> Optional[str]:
    """Make a portal tracking link safe to put in a customer email.

    Shopify requires an RFC 3986 URI with a scheme and host. The portal emits
    `http://postnl.nl/tracktrace/?...`; upgrade it to https so the shipping
    confirmation doesn't carry an insecure link.
    """
    if not url:
        return None
    url = url.strip().replace("&amp;", "&")
    if url.startswith("http://"):
        url = "https://" + url[len("http://"):]
    if not url.startswith("https://"):
        return None          # not a usable absolute URL — let Shopify decide
    return url


def carrier_for(shipper: str, country_code: Optional[str] = None) -> Optional[str]:
    """Translate a portal shipper + destination into a Shopify carrier string.

    Returns None when the shipper isn't recognised — callers should then send
    the tracking number without a company rather than guessing, because a wrong
    carrier name silently breaks the auto-generated tracking URL.
    """
    if not shipper:
        return None
    key = shipper.strip().lower()
    for prefix, (domestic, international) in _SHIPPER_MAP.items():
        if key.startswith(prefix):
            is_domestic = (country_code or "").upper() == DOMESTIC_COUNTRY
            return domestic if is_domestic else international
    return None


def build_tracking_info(number: str, company: Optional[str] = None,
                        url: Optional[str] = None) -> Dict[str, Any]:
    """Assemble trackingInfo for a fulfilment mutation.

    Shopify's own priority order for making the number clickable is:
        1. the `url` we supply
        2. a recognised `company` name (Shopify builds a generic URL)
        3. guessing from the number's format
    Their docs recommend sending company AND url, so that's what we do. Passing
    the portal's deep link matters: the carrier-derived URL is only PostNL's
    generic landing page, with no barcode in it, so the customer has to type
    the number and postcode by hand.
    """
    info: Dict[str, Any] = {"number": number}
    clean_url = normalize_tracking_url(url)
    if clean_url:
        info["url"] = clean_url
    if company:
        info["company"] = company
    elif clean_url:
        # A custom URL with no carrier name would leave the email blank about
        # who is delivering; "Other" is what the admin UI uses.
        info["company"] = OTHER_CARRIER
    return info


def _check_user_errors(payload: Dict[str, Any], mutation: str) -> None:
    """Raise if a mutation returned userErrors.

    GOTCHA: Shopify returns HTTP 200 with a populated `userErrors` array when a
    mutation is rejected. Nothing raises on its own — an unchecked bulk run
    would report success while writing nothing.
    """
    errors = (payload or {}).get("userErrors") or []
    if errors:
        detail = "; ".join(
            f"{'.'.join(e.get('field') or []) or '?'}: {e.get('message')}" for e in errors
        )
        raise RuntimeError(f"Shopify {mutation} failed: {detail}")


def get_fulfillment_orders(order_name: str) -> List[Dict[str, Any]]:
    """Return the open fulfilment orders for a shop order, e.g. "#SMYLE120579".

    Only fulfilment orders that currently support CREATE_FULFILLMENT are
    returned — anything already fulfilled, closed or held is filtered out.
    """
    data = client.graphql(
        """
        query($q: String!) {
            orders(first: 1, query: $q) {
                edges { node {
                    id name displayFulfillmentStatus
                    fulfillmentOrders(first: 10) {
                        edges { node {
                            id status requestStatus
                            assignedLocation { name }
                            supportedActions { action }
                        } }
                    }
                } }
            }
        }
        """,
        {"q": f"name:{order_name.lstrip('#')}"},
    )
    edges = data["orders"]["edges"]
    if not edges:
        return []
    return [
        fo["node"]
        for fo in edges[0]["node"]["fulfillmentOrders"]["edges"]
        if any(a["action"] == "CREATE_FULFILLMENT" for a in fo["node"]["supportedActions"])
    ]


def state_from_node(node: Dict[str, Any]) -> Dict[str, Any]:
    """Build the fulfilment state dict from an already-fetched order node.

    The node must carry `fulfillments` and `fulfillmentOrders` (with
    supportedActions). Lets a bulk listing decide what each order needs without
    a second round-trip per order — a 250-order page would otherwise cost 250
    extra GraphQL calls.
    """
    fulfillable = [
        fo["node"]
        for fo in node["fulfillmentOrders"]["edges"]
        if any(a["action"] == "CREATE_FULFILLMENT" for a in fo["node"]["supportedActions"])
    ]
    return {
        "id": node["id"],
        "name": node["name"],
        "status": node["displayFulfillmentStatus"],
        "country_code": (node.get("shippingAddress") or {}).get("countryCode"),
        "fulfillable": fulfillable,
        "fulfillments": node["fulfillments"],
    }


def get_order_state(order_name: str) -> Optional[Dict[str, Any]]:
    """Return what we need to decide how to attach tracking to an order.

    Result: {id, name, status, fulfillable[], fulfillments[]} or None if the
    order doesn't exist.

    `fulfillable` holds fulfilment orders that accept CREATE_FULFILLMENT.
    `fulfillments` holds fulfilments that already exist, each with its current
    trackingInfo — most Smyle orders are already fulfilled but carry no
    tracking, so those need update_tracking() rather than create_fulfillment().
    """
    data = client.graphql(
        """
        query($q: String!) {
            orders(first: 1, query: $q) {
                edges { node {
                    id name displayFulfillmentStatus
                    shippingAddress { countryCode }
                    fulfillments(first: 10) {
                        id name status
                        trackingInfo { company number url }
                    }
                    fulfillmentOrders(first: 10) {
                        edges { node {
                            id status requestStatus
                            assignedLocation { name }
                            supportedActions { action }
                        } }
                    }
                } }
            }
        }
        """,
        {"q": f"name:{order_name.lstrip('#')}"},
    )
    edges = data["orders"]["edges"]
    return state_from_node(edges[0]["node"]) if edges else None


def create_fulfillment(
    fulfillment_order_id: str,
    tracking_number: str,
    tracking_company: Optional[str] = None,
    tracking_url: Optional[str] = None,
    notify_customer: bool = False,
) -> Dict[str, Any]:
    """Fulfil a fulfilment order in full and attach tracking in one call.

    Omitting line items fulfils everything remaining on the fulfilment order,
    which is what we want for the whole-order shipments the 3PL sends.

    Leave `tracking_url` empty for a carrier in KNOWN_CARRIERS — Shopify builds
    the customer-facing URL from the number itself.
    """
    tracking_info = build_tracking_info(tracking_number, tracking_company, tracking_url)

    try:
        data = client.graphql(
            """
            mutation($fulfillment: FulfillmentInput!) {
                fulfillmentCreate(fulfillment: $fulfillment) {
                    fulfillment {
                        id name status createdAt
                        trackingInfo { company number url }
                    }
                    userErrors { field message }
                }
            }
            """,
            {
                "fulfillment": {
                    "lineItemsByFulfillmentOrder": [
                        {"fulfillmentOrderId": fulfillment_order_id}
                    ],
                    "trackingInfo": tracking_info,
                    "notifyCustomer": notify_customer,
                }
            },
        )
        payload = data["fulfillmentCreate"]
        _check_user_errors(payload, "fulfillmentCreate")
    except Exception as exc:
        audit.record("create", fulfillment_order_id, tracking_number, tracking_company,
                     tracking_info.get("url"), notify_customer, False, str(exc)[:500])
        raise

    fulfillment = payload["fulfillment"]
    audit.record("create", fulfillment_order_id, tracking_number, tracking_company,
                 tracking_info.get("url"), notify_customer, True,
                 fulfillment.get("name", ""), _order_name(fulfillment.get("name", "")))
    log.info(
        "Created fulfilment %s with tracking %s (%s), notify=%s",
        fulfillment["name"], tracking_number, tracking_company or "no carrier",
        notify_customer,
    )
    return fulfillment


def update_tracking(
    fulfillment_id: str,
    tracking_number: str,
    tracking_company: Optional[str] = None,
    tracking_url: Optional[str] = None,
    notify_customer: bool = False,
) -> Dict[str, Any]:
    """Set or correct tracking on a fulfilment that already exists."""
    tracking_info = build_tracking_info(tracking_number, tracking_company, tracking_url)

    try:
        data = client.graphql(
            """
            mutation($fulfillmentId: ID!, $trackingInfoInput: FulfillmentTrackingInput!,
                     $notifyCustomer: Boolean) {
                fulfillmentTrackingInfoUpdateV2(
                    fulfillmentId: $fulfillmentId,
                    trackingInfoInput: $trackingInfoInput,
                    notifyCustomer: $notifyCustomer
                ) {
                    fulfillment { id name trackingInfo { company number url } }
                    userErrors { field message }
                }
            }
            """,
            {
                "fulfillmentId": fulfillment_id,
                "trackingInfoInput": tracking_info,
                "notifyCustomer": notify_customer,
            },
        )
        payload = data["fulfillmentTrackingInfoUpdateV2"]
        _check_user_errors(payload, "fulfillmentTrackingInfoUpdateV2")
    except Exception as exc:
        audit.record("update", fulfillment_id, tracking_number, tracking_company,
                     tracking_info.get("url"), notify_customer, False, str(exc)[:500])
        raise

    fulfillment = payload["fulfillment"]
    audit.record("update", fulfillment_id, tracking_number, tracking_company,
                 tracking_info.get("url"), notify_customer, True,
                 fulfillment.get("name", ""), _order_name(fulfillment.get("name", "")))
    log.info("Updated tracking on %s to %s", fulfillment["name"], tracking_number)
    return fulfillment
