"""
Tiny local settings store for automation config values.

We use a SQLite file to persist settings such as:
    - SPREAD_SHEET_NAME
    - WORK_SHEET_NAME
    - ORDER_TYPE_SHEET_URL

On first access, if a value is missing in the DB but present in the environment,
the env value is seeded into the DB so existing setups keep working without
manual re-entry.
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Dict, Iterable, Optional

PROJECT_ROOT = Path(__file__).resolve().parent
DB_PATH = PROJECT_ROOT / "config.db"


def _get_conn() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """
    )
    return conn


def get_setting(key: str, default: Optional[str] = None) -> Optional[str]:
    """Return a single setting value, seeding from env if empty."""
    with _get_conn() as conn:
        cur = conn.execute("SELECT value FROM settings WHERE key = ?", (key,))
        row = cur.fetchone()
        if row:
            return row[0]

        # Seed from environment if present
        env_val = os.getenv(key)
        if env_val is not None:
            set_setting(key, env_val)
            return env_val
        return default


def get_settings(keys: Iterable[str]) -> Dict[str, Optional[str]]:
    """Return multiple settings as a dict."""
    return {key: get_setting(key) for key in keys}


def set_setting(key: str, value: Optional[str]) -> None:
    """Persist a single setting value."""
    with _get_conn() as conn:
        conn.execute(
            "REPLACE INTO settings (key, value) VALUES (?, ?)",
            (key, value if value is not None else ""),
        )
        conn.commit()


def set_settings(data: Dict[str, Optional[str]]) -> None:
    """Persist multiple settings in one transaction."""
    with _get_conn() as conn:
        conn.executemany(
            "REPLACE INTO settings (key, value) VALUES (?, ?)",
            [
                (key, val if val is not None else "")
                for key, val in data.items()
            ],
        )
        conn.commit()
