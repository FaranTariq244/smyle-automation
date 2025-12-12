"""
Lightweight scheduler to run automation tasks on a recurrence.

The scheduler persists its configuration in the existing config.db so that
scheduled runs survive restarts. It intentionally avoids touching the business
logic of the report scripts; it only decides *when* to trigger them.
"""

from __future__ import annotations

import sqlite3
import threading
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Callable, Dict, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parent
DB_PATH = PROJECT_ROOT / "config.db"

# Supported recurrence options exposed to the UI
RECURRENCE_CHOICES = ("hourly", "daily", "weekly", "every_4_days", "monthly")
DEFAULT_TIME_OF_DAY = "07:00"


# --- helpers ---------------------------------------------------------------

def _dict_factory(cursor, row):
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}


def _parse_time_str(value: str) -> time:
    try:
        parts = value.split(":")
        hour = int(parts[0])
        minute = int(parts[1]) if len(parts) > 1 else 0
        return time(hour=hour, minute=minute)
    except Exception:
        return time(hour=7, minute=0)


def _parse_date_str(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except Exception:
        return datetime.now().date()


def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


def _format_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _bump_month(dt: datetime) -> datetime:
    month = dt.month + 1
    year = dt.year
    if month > 12:
        month = 1
        year += 1
    # Clamp day to the last day of the target month
    last_day_lookup = [
        31,
        29 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0) else 28,
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ]
    day = min(dt.day, last_day_lookup[month - 1])
    return dt.replace(year=year, month=month, day=day)


def _increment(dt: datetime, recurrence: str) -> datetime:
    if recurrence == "hourly":
        return dt + timedelta(hours=1)
    if recurrence == "daily":
        return dt + timedelta(days=1)
    if recurrence == "weekly":
        return dt + timedelta(days=7)
    if recurrence == "every_4_days":
        return dt + timedelta(days=4)
    if recurrence == "monthly":
        return _bump_month(dt)
    raise ValueError(f"Unsupported recurrence: {recurrence}")


def _first_run_dt(
    recurrence: str,
    time_of_day: str,
    start_date: str,
    reference: Optional[datetime] = None,
) -> datetime:
    reference = reference or datetime.now()
    start = _parse_date_str(start_date)
    tod = _parse_time_str(time_of_day or DEFAULT_TIME_OF_DAY)
    candidate = datetime.combine(start, tod)
    while candidate <= reference:
        candidate = _increment(candidate, recurrence)
    return candidate


def compute_next_run(
    current_next: Optional[str],
    recurrence: str,
    time_of_day: str,
    start_date: str,
    reference: Optional[datetime] = None,
) -> datetime:
    """Return the next run datetime after reference (defaults to now)."""
    reference = reference or datetime.now()
    next_dt = _parse_dt(current_next)
    if not next_dt:
        next_dt = _first_run_dt(recurrence, time_of_day, start_date, reference)
    while next_dt <= reference:
        next_dt = _increment(next_dt, recurrence)
    return next_dt


# --- persistence -----------------------------------------------------------


class ScheduleStore:
    """SQLite-backed schedule storage that shares the existing config.db."""

    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self._ensure_table()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = _dict_factory
        return conn

    def _ensure_table(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS schedules (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    key TEXT UNIQUE,
                    name TEXT NOT NULL,
                    task TEXT NOT NULL,
                    recurrence TEXT NOT NULL,
                    time_of_day TEXT NOT NULL,
                    start_date TEXT NOT NULL,
                    run_for_days_ago INTEGER NOT NULL DEFAULT 1,
                    next_run TEXT,
                    last_run TEXT,
                    last_status TEXT,
                    last_message TEXT,
                    last_log_path TEXT,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """CREATE INDEX IF NOT EXISTS idx_schedules_next_run ON schedules(next_run)"""
            )

    def upsert_schedule(
        self,
        *,
        key: str,
        name: str,
        task: str,
        recurrence: str,
        time_of_day: str,
        start_date: str,
        run_for_days_ago: int = 1,
        enabled: bool = True,
    ) -> Dict:
        if recurrence not in RECURRENCE_CHOICES:
            raise ValueError(f"recurrence must be one of {RECURRENCE_CHOICES}")
        now = datetime.now()
        next_run = compute_next_run(None, recurrence, time_of_day, start_date, now)
        payload = {
            "key": key,
            "name": name,
            "task": task,
            "recurrence": recurrence,
            "time_of_day": time_of_day,
            "start_date": start_date,
            "run_for_days_ago": max(0, int(run_for_days_ago)),
            "next_run": _format_dt(next_run),
            "enabled": 1 if enabled else 0,
            "created_at": _format_dt(now),
        }
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO schedules (key, name, task, recurrence, time_of_day, start_date, run_for_days_ago, next_run, enabled, created_at)
                VALUES (:key, :name, :task, :recurrence, :time_of_day, :start_date, :run_for_days_ago, :next_run, :enabled, :created_at)
                ON CONFLICT(key) DO UPDATE SET
                    name=excluded.name,
                    task=excluded.task,
                    recurrence=excluded.recurrence,
                    time_of_day=excluded.time_of_day,
                    start_date=excluded.start_date,
                    run_for_days_ago=excluded.run_for_days_ago,
                    next_run=excluded.next_run,
                    enabled=excluded.enabled
                """,
                payload,
            )
        return self.get_by_key(key)  # type: ignore

    def list_schedules(self) -> List[Dict]:
        with self._connect() as conn:
            cur = conn.execute("SELECT * FROM schedules ORDER BY created_at ASC")
            return cur.fetchall()

    def get_by_key(self, key: str) -> Optional[Dict]:
        with self._connect() as conn:
            cur = conn.execute("SELECT * FROM schedules WHERE key = ?", (key,))
            return cur.fetchone()

    def get(self, schedule_id: int) -> Optional[Dict]:
        with self._connect() as conn:
            cur = conn.execute("SELECT * FROM schedules WHERE id = ?", (schedule_id,))
            return cur.fetchone()

    def set_enabled(self, schedule_id: int, enabled: bool) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE schedules SET enabled = ? WHERE id = ?",
                (1 if enabled else 0, schedule_id),
            )

    def delete_schedule(self, schedule_id: int) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM schedules WHERE id = ?", (schedule_id,))

    def due_schedules(self, reference: Optional[datetime] = None) -> List[Dict]:
        reference = reference or datetime.now()
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT * FROM schedules WHERE enabled = 1 AND next_run IS NOT NULL"
            )
            rows = cur.fetchall()
        due = []
        for row in rows:
            next_dt = _parse_dt(row.get("next_run"))
            if next_dt and next_dt <= reference:
                due.append(row)
        return due

    def bump_next_run(self, schedule_id: int, after: Optional[datetime] = None) -> Optional[str]:
        job = self.get(schedule_id)
        if not job:
            return None
        after = after or datetime.now()
        next_dt = compute_next_run(
            job.get("next_run"),
            job["recurrence"],
            job["time_of_day"],
            job["start_date"],
            after,
        )
        next_str = _format_dt(next_dt)
        with self._connect() as conn:
            conn.execute(
                "UPDATE schedules SET next_run = ? WHERE id = ?",
                (next_str, schedule_id),
            )
        return next_str

    def mark_running(self, schedule_id: int, message: str | None = None) -> None:
        now_str = _format_dt(datetime.now())
        with self._connect() as conn:
            conn.execute(
                "UPDATE schedules SET last_run = ?, last_status = 'running', last_message = ? WHERE id = ?",
                (now_str, message or "", schedule_id),
            )

    def mark_completed(
        self,
        schedule_id: int,
        success: bool,
        message: str | None = None,
        log_path: Optional[str] = None,
    ) -> Optional[str]:
        job = self.get(schedule_id)
        if not job:
            return None
        next_dt = compute_next_run(
            job.get("next_run"),
            job["recurrence"],
            job["time_of_day"],
            job["start_date"],
            datetime.now(),
        )
        next_str = _format_dt(next_dt)
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE schedules
                SET last_run = ?, last_status = ?, last_message = ?, last_log_path = ?, next_run = ?
                WHERE id = ?
                """,
                (
                    _format_dt(datetime.now()),
                    "success" if success else "failed",
                    message or "",
                    log_path or job.get("last_log_path"),
                    next_str,
                    schedule_id,
                ),
            )
        return next_str


# --- scheduler runtime -----------------------------------------------------


class SchedulerService:
    """Background checker that triggers due schedules via a callback."""

    def __init__(
        self,
        store: ScheduleStore,
        on_job_due: Callable[[Dict], None],
        *,
        can_start: Optional[Callable[[], bool]] = None,
        log: Optional[Callable[[str], None]] = None,
        poll_seconds: int = 30,
    ):
        self.store = store
        self.on_job_due = on_job_due
        self.can_start = can_start
        self.log = log or (lambda msg: None)
        self.poll_seconds = poll_seconds
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._active_schedule_id: Optional[int] = None

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=1)

    def mark_run_complete(
        self, schedule_id: int, success: bool, message: str | None = None, log_path: Optional[str] = None
    ) -> Optional[str]:
        if self._active_schedule_id == schedule_id:
            self._active_schedule_id = None
        return self.store.mark_completed(schedule_id, success, message, log_path)

    def refresh_next_run(self, schedule_id: int) -> Optional[str]:
        return self.store.bump_next_run(schedule_id)

    def _run_loop(self):
        while not self._stop_event.is_set():
            if self._active_schedule_id:
                self._stop_event.wait(self.poll_seconds)
                continue
            if self.can_start and not self.can_start():
                self._stop_event.wait(self.poll_seconds)
                continue
            now = datetime.now()
            due_jobs = self.store.due_schedules(now)
            if not due_jobs:
                self._stop_event.wait(self.poll_seconds)
                continue
            job = due_jobs[0]
            try:
                started = bool(self.on_job_due(job))
                if started:
                    self._active_schedule_id = job["id"]
                else:
                    self._active_schedule_id = None
            except Exception as exc:  # pragma: no cover - defensive
                self.log(f"Failed to start scheduled job {job.get('name')}: {exc}")
                self._active_schedule_id = None
                self.store.mark_completed(job["id"], False, f"Failed to start: {exc}")
            self._stop_event.wait(self.poll_seconds)
