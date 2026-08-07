"""
Local cache of my-fulfilment.com order details.

Fetching a detail page costs ~0.25s and there is no bulk export that works
(POST /orders/export returns 500), so a month-wide comparison means thousands
of sequential-ish page loads. Most of that work is repeated every time an
overlapping range is queried.

Once an order reaches **Completed** its packages and T&T codes never change,
so those rows are safe to cache forever. Anything not yet Completed is not
cached — it is still moving.

Stored in the project's existing config.db, in its own table.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional

log = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_ROOT / "config.db"

# Only terminal states are cacheable — everything else can still change.
CACHEABLE_STATUSES = {"Completed"}

_lock = threading.Lock()


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS portal_detail (
            reference     TEXT PRIMARY KEY,
            status        TEXT,
            tnt_codes     TEXT,      -- comma separated
            shipper       TEXT,
            delivery_mode TEXT,
            wms_ref       TEXT,
            error_message TEXT,
            packages      TEXT,      -- JSON
            cached_at     TEXT
        )
        """
    )
    # Grid-level fields, added so a cached single-order lookup can answer
    # completely without touching the portal — the reference search that would
    # otherwise be needed is an unindexed substring scan that can take minutes.
    cols = {r[1] for r in conn.execute("PRAGMA table_info(portal_detail)")}
    for name in ("detail_url", "country", "created_at", "modified_at", "tracking_url"):
        if name not in cols:
            conn.execute(f"ALTER TABLE portal_detail ADD COLUMN {name} TEXT")
    return conn


def get_many(references: Iterable[str]) -> Dict[str, dict]:
    """Return cached rows for whichever references we already know."""
    refs = list(references)
    if not refs:
        return {}
    out: Dict[str, dict] = {}
    try:
        with _lock, _conn() as conn:
            # Chunked so a huge range can't blow SQLite's variable limit.
            for i in range(0, len(refs), 500):
                chunk = refs[i:i + 500]
                q = ("SELECT reference, status, tnt_codes, shipper, delivery_mode, "
                     "wms_ref, error_message, detail_url, country, created_at, "
                     "modified_at, tracking_url FROM portal_detail WHERE reference IN "
                     f"({','.join('?' * len(chunk))})")
                for row in conn.execute(q, chunk):
                    out[row[0]] = {
                        "portal_status": row[1],
                        "portal_tracking": row[2] or "",
                        "shipper": row[3] or "",
                        "delivery_mode": row[4] or "",
                        "wms_ref": row[5] or "",
                        "portal_error": row[6] or "",
                        "detail_url": row[7] or "",
                        "portal_country": row[8] or "",
                        "portal_created": row[9] or "",
                        "portal_modified": row[10] or "",
                        "tracking_url": row[11] or "",
                    }
    except sqlite3.Error as exc:
        # A cache problem must never break a lookup — just miss.
        log.warning("Portal cache read failed (%s) — continuing without it", exc)
        return {}
    return out


def put(reference: str, detail, row=None) -> None:
    """Cache one detail, but only if the order has reached a terminal state.

    `row` is the grid OrderRow it came from, when available — storing its
    detail_url lets a later single-order lookup skip the slow reference search
    entirely and go straight to the 0.25s detail page.
    """
    status = detail.fields.get("Status", "")
    if status not in CACHEABLE_STATUSES:
        return
    packages = [
        {"shipping_date": p.shipping_date, "shipper": p.shipper,
         "shipping_method": p.shipping_method, "weight": p.weight,
         "tnt_code": p.tnt_code, "tnt_url": p.tnt_url}
        for p in detail.packages
    ]
    tracked = detail.tracked_packages
    try:
        with _lock, _conn() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO portal_detail (reference, status, tnt_codes, "
                "shipper, delivery_mode, wms_ref, error_message, packages, cached_at, "
                "detail_url, country, created_at, modified_at, tracking_url) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    reference, status,
                    ", ".join(p.tnt_code for p in tracked),
                    ", ".join(sorted({p.shipper for p in tracked}))
                    or detail.fields.get("Shipper", ""),
                    detail.fields.get("Delivery mode", ""),
                    detail.fields.get("Nic. Oud reference", ""),
                    detail.error_message or "",
                    json.dumps(packages),
                    datetime.now().isoformat(timespec="seconds"),
                    getattr(row, "detail_url", "") or detail.url or "",
                    getattr(row, "country", "") or detail.fields.get("Country", ""),
                    getattr(row, "created_at", "") or detail.fields.get("Created at", ""),
                    getattr(row, "modified_at", "") or detail.fields.get("Modified at", ""),
                    # The portal's own deep link — it carries the postcode and
                    # country PostNL needs, which a carrier-derived URL doesn't.
                    tracked[0].tnt_url if tracked else "",
                ),
            )
    except sqlite3.Error as exc:
        log.warning("Portal cache write failed for %s (%s)", reference, exc)


def put_many(details: Dict[str, object], rows_by_ref: Optional[Dict[str, object]] = None) -> None:
    rows_by_ref = rows_by_ref or {}
    for ref, detail in details.items():
        if detail is not None:
            put(ref, detail, rows_by_ref.get(ref))


def stats() -> dict:
    try:
        with _lock, _conn() as conn:
            n = conn.execute("SELECT COUNT(*) FROM portal_detail").fetchone()[0]
            newest = conn.execute(
                "SELECT MAX(cached_at) FROM portal_detail").fetchone()[0]
        return {"rows": n, "newest": newest}
    except sqlite3.Error:
        return {"rows": 0, "newest": None}


def clear() -> int:
    with _lock, _conn() as conn:
        n = conn.execute("SELECT COUNT(*) FROM portal_detail").fetchone()[0]
        conn.execute("DELETE FROM portal_detail")
    return n
