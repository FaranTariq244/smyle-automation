"""
Audit log for every write this project makes to Shopify.

Deliberately hooked into `services/shopify/fulfillment.py` at the mutation
itself, not at the callers — so the UI push, the bulk push, the scheduled sync
and the CLI are all recorded without anyone having to remember to log. A write
that isn't in here didn't come from us.

Two destinations, because they answer different questions:
  * SQLite (config.db, table `shopify_writes`) — queryable: what happened to
    this order, what did we change on this day, what failed
  * logs/tracking_writes.log — an append-only text trail that survives even if
    the database is replaced, and is easy to hand to someone else

Both record failures as well as successes. A write that errored is exactly the
thing you want to find later.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

log = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_ROOT / "config.db"
LOG_DIR = PROJECT_ROOT / "logs"
LOG_FILE = LOG_DIR / "tracking_writes.log"

_lock = threading.Lock()
_context = threading.local()


def set_origin(origin: str) -> None:
    """Label writes made by this thread, e.g. 'ui-single', 'bulk', 'scheduled'."""
    _context.origin = origin


def get_origin() -> str:
    return getattr(_context, "origin", "unknown")


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS shopify_writes (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            at           TEXT NOT NULL,
            origin       TEXT,
            action       TEXT,          -- create | update
            order_name   TEXT,
            target_id    TEXT,          -- fulfilment order / fulfilment gid
            tracking     TEXT,
            carrier      TEXT,
            tracking_url TEXT,
            notified     INTEGER,       -- was the customer emailed
            ok           INTEGER,
            result       TEXT           -- fulfilment name, or the error
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_writes_order ON shopify_writes(order_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_writes_at ON shopify_writes(at)")
    return conn


def record(action: str, target_id: str, tracking: str, carrier: Optional[str],
           tracking_url: Optional[str], notified: bool, ok: bool,
           result: str, order_name: str = "") -> None:
    """Write one audit row. Never raises — auditing must not break a write."""
    stamp = datetime.now().isoformat(timespec="seconds")
    origin = get_origin()
    try:
        with _lock:
            with _conn() as conn:
                conn.execute(
                    "INSERT INTO shopify_writes (at, origin, action, order_name, "
                    "target_id, tracking, carrier, tracking_url, notified, ok, result) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    (stamp, origin, action, order_name, target_id, tracking,
                     carrier or "", tracking_url or "", int(notified), int(ok), result),
                )
            LOG_DIR.mkdir(exist_ok=True)
            with LOG_FILE.open("a", encoding="utf-8") as fh:
                fh.write(
                    f"{stamp}\t{origin}\t{action}\t{order_name or target_id}\t"
                    f"{tracking}\t{carrier or '-'}\t"
                    f"notify={'yes' if notified else 'no'}\t"
                    f"{'OK' if ok else 'FAIL'}\t{result}\n"
                )
    except Exception as exc:            # noqa: BLE001 - audit must never throw
        log.warning("Could not record Shopify write for %s: %s",
                    order_name or target_id, exc)


def recent(limit: int = 100, order_name: str = "", since: str = "",
           failures_only: bool = False) -> List[Dict[str, Any]]:
    """Most recent writes, newest first."""
    clauses, params = [], []
    if order_name:
        clauses.append("order_name LIKE ?")
        params.append(f"%{order_name.lstrip('#')}%")
    if since:
        clauses.append("at >= ?")
        params.append(since)
    if failures_only:
        clauses.append("ok = 0")
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    try:
        with _lock, _conn() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                f"SELECT * FROM shopify_writes {where} ORDER BY id DESC LIMIT ?",
                (*params, int(limit)),
            ).fetchall()
        return [dict(r) for r in rows]
    except sqlite3.Error as exc:
        log.warning("Could not read the write log: %s", exc)
        return []


def summary() -> Dict[str, Any]:
    """Counts for the whole log, plus today's."""
    today = datetime.now().strftime("%Y-%m-%d")
    try:
        with _lock, _conn() as conn:
            total, ok, emailed = conn.execute(
                "SELECT COUNT(*), COALESCE(SUM(ok),0), COALESCE(SUM(notified),0) "
                "FROM shopify_writes").fetchone()
            today_n = conn.execute(
                "SELECT COUNT(*) FROM shopify_writes WHERE at LIKE ?",
                (f"{today}%",)).fetchone()[0]
            last = conn.execute("SELECT MAX(at) FROM shopify_writes").fetchone()[0]
        return {"total": total, "succeeded": ok, "failed": total - ok,
                "emailed": emailed, "today": today_n, "last": last}
    except sqlite3.Error:
        return {"total": 0, "succeeded": 0, "failed": 0,
                "emailed": 0, "today": 0, "last": None}
