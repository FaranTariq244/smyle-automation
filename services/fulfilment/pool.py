"""
Parallel reads against my-fulfilment.com.

The portal serialises every request that shares a PHPSESSID — PHP locks the
session file for the duration of each request. Measured: 6 grid pages took
6.12s sequentially and 6.33s across 3 threads on one session (0.97x, i.e. no
gain at all). With three separate logins the same 6 pages took 2.56s (2.39x).

So concurrency here is not about threads, it's about **independent sessions**.
This module owns a small pool of logged-in clients and hands one to each
worker thread.

Deliberately small: the portal is a third party's production system and its
own export endpoint already 500s under load, so the default is 3 sessions and
MAX_SESSIONS caps it at 4.

Everything here is read-only.
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Iterable, List, Optional

import requests

from . import client as myf

log = logging.getLogger(__name__)

DEFAULT_SESSIONS = 3
MAX_SESSIONS = 4

# The portal intermittently answers 500/503 when busy. Retry a few times with
# backoff rather than losing the order from the result set.
RETRY_STATUSES = (500, 502, 503, 504)
MAX_ATTEMPTS = 3


class SessionPool:
    """A handful of independently authenticated portal clients.

    Each worker thread gets its own client via thread-local assignment, so no
    two threads ever share a PHPSESSID.
    """

    def __init__(self, size: int = DEFAULT_SESSIONS):
        self.size = max(1, min(size, MAX_SESSIONS))
        self._clients: List[myf.MyFulfilmentClient] = []
        self._local = threading.local()
        self._lock = threading.Lock()
        self._next = 0

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, *exc):
        self.close()

    def open(self) -> None:
        started = time.perf_counter()
        for i in range(self.size):
            c = myf.MyFulfilmentClient(cookie_file=f"{myf.COOKIE_FILE}.pool{i}")
            c.login()
            self._clients.append(c)
        log.info("Portal session pool ready — %d session(s) in %.1fs",
                 len(self._clients), time.perf_counter() - started)

    def close(self) -> None:
        for c in self._clients:
            try:
                c.save_session()
                c.session.close()
            except Exception:
                pass
        self._clients = []

    def client(self) -> myf.MyFulfilmentClient:
        """The calling thread's dedicated client."""
        got = getattr(self._local, "client", None)
        if got is None:
            with self._lock:
                got = self._clients[self._next % len(self._clients)]
                self._next += 1
            self._local.client = got
        return got

    # -- resilient wrappers --------------------------------------------------

    def _retry(self, what: str, fn: Callable, *args, **kwargs):
        """Run a portal call, retrying transient server errors."""
        last: Optional[Exception] = None
        for attempt in range(MAX_ATTEMPTS):
            try:
                return fn(*args, **kwargs)
            except requests.HTTPError as exc:
                code = exc.response.status_code if exc.response is not None else 0
                if code not in RETRY_STATUSES or attempt == MAX_ATTEMPTS - 1:
                    raise
                last = exc
            except myf.LoginError:
                # Session expired mid-run — re-authenticate this client only.
                self.client().login(force=True)
            except requests.RequestException as exc:
                if attempt == MAX_ATTEMPTS - 1:
                    raise
                last = exc
            wait = 1.5 * (2 ** attempt)
            log.warning("Portal %s failed (%s) — retrying in %.1fs", what, last, wait)
            time.sleep(wait)
        raise RuntimeError(f"Portal {what} failed after {MAX_ATTEMPTS} attempts")

    def list_page(self, page: int, **filters):
        return self._retry(f"grid page {page}", self.client().list_orders,
                           page=page, **filters)

    def detail(self, row):
        return self._retry(f"detail {row.reference}",
                           self.client().get_order_detail, row)

    # -- bulk operations -----------------------------------------------------

    def list_range(self, date_from: str, date_to: str,
                   page_size: int = 10, max_pages: int = 2000,
                   progress: Optional[Callable[[int], None]] = None) -> List:
        """Every order created in a date window, newest pages first.

        Uses the DATE FILTER ONLY. Measured per grid page: 0.95s with just the
        date filter, 5.0s adding statusCode, 6.2s adding the sort as well — so
        status and ordering are applied in Python afterwards, which is free.
        Verified that unsorted paging is stable: two identical walks returned
        the same references with zero duplicates.

        Pages are fetched in waves of `pool.size`, stopping at the first empty
        page in a wave.
        """
        rows: List = []
        seen = set()
        page = 1

        with ThreadPoolExecutor(max_workers=self.size) as ex:
            while page <= max_pages:
                wave = list(range(page, page + self.size))
                batches = list(ex.map(
                    lambda p: self._safe_page(p, date_from, date_to), wave))

                stop = False
                for batch in batches:
                    if not batch:
                        stop = True
                        break
                    fresh = [r for r in batch if r.reference not in seen]
                    if not fresh:
                        stop = True     # pager clamped to the last page
                        break
                    seen.update(r.reference for r in fresh)
                    rows.extend(fresh)

                if progress:
                    progress(len(rows))
                if stop:
                    break
                page += self.size

        if page > max_pages:
            log.warning("Stopped at the %d-page cap with %d order(s) — there may "
                        "be more.", max_pages, len(rows))
        return rows

    def _safe_page(self, page: int, date_from: str, date_to: str) -> List:
        """A page that returns [] instead of raising, so one bad page can't
        abort the whole walk."""
        try:
            return self.list_page(page, created_from=date_from, created_to=date_to)
        except Exception as exc:
            log.error("Portal grid page %d failed permanently: %s", page, exc)
            return []

    def details(self, rows: Iterable, progress: Optional[Callable[[int, int], None]] = None):
        """Fetch details for many rows concurrently.

        Returns {reference: OrderDetail}. A row that fails after retries is
        omitted rather than killing the batch — callers report it as unknown
        instead of silently treating it as "no tracking".
        """
        rows = list(rows)
        out = {}
        done = 0

        def one(row):
            try:
                return row.reference, self.detail(row)
            except Exception as exc:
                log.error("Portal detail for %s failed: %s", row.reference, exc)
                return row.reference, None

        with ThreadPoolExecutor(max_workers=self.size) as ex:
            for ref, detail in ex.map(one, rows):
                done += 1
                if detail is not None:
                    out[ref] = detail
                if progress and done % 25 == 0:
                    progress(done, len(rows))

        if progress:
            progress(done, len(rows))
        return out
