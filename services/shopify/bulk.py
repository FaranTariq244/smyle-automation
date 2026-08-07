"""
Fast bulk reads from the Shopify Admin API.

Two measured facts drive everything here:

1. **Page size 250 is the worst choice.** With the nested fulfilments /
   fulfilmentOrders connections, time per order roughly doubles from 50 to 250
   rows: 50 rows 2.3s, 100 rows 5.3s, 250 rows 25.1s. 100 is the sweet spot.
   (The connections are the whole cost — 250 bare id/name rows take 0.70s.)

2. **Cursor pages can't be parallelised, date ranges can.** You need page N's
   cursor to request N+1, so instead we split the window into per-day queries
   and run those concurrently: 4 day-queries took 11.0s sequentially, 2.8s
   across 4 threads (3.88x).

Rate limits are not a constraint at this scale — the bucket is 20,000 points
restoring at 1,000/s, and a 100-order page costs ~64. Four parallel 250-row
queries only drew the bucket down to 19,751. Throttling is still handled, in
client.graphql(), because being wrong about that fails a whole run.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from typing import Callable, Dict, Iterable, List, Optional

from . import client

log = logging.getLogger(__name__)

# Measured optimum — see module docstring.
PAGE_SIZE = 100

# Shopify's search grammar handles this many OR terms comfortably; 40 orders
# came back in 1.8s (0.045s each).
NAME_BATCH = 40

# Concurrency for independent queries. Well inside the throttle budget.
WORKERS = 4


def _daterange(date_from: str, date_to: str) -> List[str]:
    start, end = date.fromisoformat(date_from), date.fromisoformat(date_to)
    return [(start + timedelta(days=i)).isoformat()
            for i in range((end - start).days + 1)]


def fetch_range(date_from: str, date_to: str, fields: str,
                timestamp_fn: Callable[[str], str],
                workers: int = WORKERS,
                progress: Optional[Callable[[int], None]] = None) -> List[dict]:
    """Every order created in a window, as raw GraphQL nodes.

    The window is split into single days that are fetched concurrently; each
    day is cursor-paginated internally. `timestamp_fn` turns a YYYY-MM-DD into
    a full shop-time ISO timestamp (a date-only bound is silently ignored by
    Shopify's order search).
    """
    days = _daterange(date_from, date_to)
    query = """
    query($q: String!, $after: String) {
      orders(first: %d, query: $q, sortKey: CREATED_AT, reverse: true, after: $after) {
        pageInfo { hasNextPage endCursor }
        edges { node { %s } }
      }
    }
    """ % (PAGE_SIZE, fields)

    def one_day(day: str) -> List[dict]:
        start = timestamp_fn(day)
        end = timestamp_fn((date.fromisoformat(day) + timedelta(days=1)).isoformat())
        q = f"created_at:>='{start}' AND created_at:<'{end}'"
        nodes, after = [], None
        while True:
            try:
                conn = client.graphql(query, {"q": q, "after": after})["orders"]
            except Exception as exc:
                # One bad day shouldn't lose the rest of the range.
                log.error("Shopify fetch failed for %s: %s", day, exc)
                return nodes
            nodes += [e["node"] for e in conn["edges"]]
            if not conn["pageInfo"]["hasNextPage"]:
                return nodes
            after = conn["pageInfo"]["endCursor"]

    out: List[dict] = []
    with ThreadPoolExecutor(max_workers=min(workers, max(len(days), 1))) as ex:
        for chunk in ex.map(one_day, days):
            out.extend(chunk)
            if progress:
                progress(len(out))

    out.sort(key=lambda n: n.get("createdAt", ""), reverse=True)
    return out


def fetch_by_names(names: Iterable[str], fields: str,
                   workers: int = WORKERS) -> Dict[str, dict]:
    """Fetch specific orders by name, batched and in parallel.

    Returns {order_name: node}. Far cheaper than listing a whole range when you
    already know which orders you care about — e.g. only the ones the portal
    says carry a tracking number.
    """
    clean = [str(n).lstrip("#").strip() for n in names if str(n).strip()]
    if not clean:
        return {}

    batches = [clean[i:i + NAME_BATCH] for i in range(0, len(clean), NAME_BATCH)]
    query = """
    query($q: String!) {
      orders(first: %d, query: $q) { edges { node { %s } } }
    }
    """ % (NAME_BATCH, fields)

    def one_batch(batch: List[str]) -> List[dict]:
        q = " OR ".join(f"name:{n}" for n in batch)
        try:
            data = client.graphql(query, {"q": q})
            return [e["node"] for e in data["orders"]["edges"]]
        except Exception as exc:
            log.error("Shopify name batch failed (%s…): %s", batch[0], exc)
            return []

    out: Dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=min(workers, len(batches))) as ex:
        for nodes in ex.map(one_batch, batches):
            for node in nodes:
                out[node["name"]] = out.get(node["name"], node)
    return out
