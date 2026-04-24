"""
SMYLE_ONLINE_STRATEGY_RN_FC1 (Weekly) - BigQuery data extractor.

Replaces the Looker Studio scraping approach with direct BigQuery queries.
No browser/Chrome needed.

Phase 2 totals (Net Revenue, Net Rev. FT, Net Rev. RP):
    -> orders_enriched_agg_spend table

Phases 3-8 Marketing Deepdive KPIs (10 metrics per medium/country combo):
    -> ads_online table

Values are rounded to match the sheet conventions:
    - impressions, clicks, orders, turnover, spend → integer
    - aov, cpo → integer
    - roas → 2 decimal places
    - ctr, cr → percentage string ("1.89%") for proper sheet formatting
"""

from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional, Union

from services.bigquery.client import fqn, run_query


def _round_opt(val, ndigits=0):
    """Round a value if not None. ndigits=0 returns int."""
    if val is None:
        return None
    r = round(float(val), ndigits)
    return int(r) if ndigits == 0 else r


def _pct_str(val):
    """Format a ratio as a percentage string for Google Sheets (e.g. 0.0189 -> '1.89%').

    USER_ENTERED will interpret '1.89%' as 0.0189 with percentage cell format.
    """
    if val is None:
        return None
    return f"{float(val) * 100:.2f}%"


# ---------------------------------------------------------------------------
# Phase 2: Net Revenue totals
# ---------------------------------------------------------------------------

def extract_weekly_totals_bq(
    start_date: datetime,
    end_date: datetime,
) -> Dict[str, Optional[float]]:
    """Query Net Revenue (Total), Net Rev. FT (New), Net Rev. RP (Recurring).

    Uses orders_enriched_agg_spend which has pre-aggregated first-time
    and returning revenue columns.

    Returns dict with keys: 'total', 'new', 'recurring'. Values are rounded.
    """
    sql = f"""
    SELECT
      SUM(net_revenue)            AS total,
      SUM(netrevenue_first_time)  AS first_time,
      SUM(netrevenue_returning)   AS recurring
    FROM {fqn('orders_enriched_agg_spend')}
    WHERE date BETWEEN '{start_date:%Y-%m-%d}' AND '{end_date:%Y-%m-%d}'
    """
    rows = run_query(sql)
    if not rows:
        return {"total": None, "new": None, "recurring": None}

    row = rows[0]
    return {
        "total": _round_opt(row.get("total")),
        "new": _round_opt(row.get("first_time")),
        "recurring": _round_opt(row.get("recurring")),
    }


# ---------------------------------------------------------------------------
# Phases 3-8: Marketing Deepdive KPIs from ads_online
# ---------------------------------------------------------------------------

def extract_marketing_kpis_bq(
    start_date: datetime,
    end_date: datetime,
    medium: Optional[str] = None,
    countries: Optional[Union[str, List[str]]] = None,
    exclude_countries: bool = False,
) -> Dict[str, Optional[float]]:
    """Query the 10 Marketing Deepdive KPIs from ads_online.

    Args:
        start_date, end_date: date range (inclusive).
        medium: 'Facebook' or 'Google Ads'. None = all mediums.
        countries: single country name, list of country names, or None (all).
        exclude_countries: if True, select all countries EXCEPT those listed.

    Returns dict with keys:
        impressions, clicks, ctr, orders, cr, turnover, spend, aov, cpo, roas

    Values are rounded to match sheet conventions:
        - impressions, clicks, orders, turnover, spend -> int
        - aov, cpo -> int
        - roas -> 2 decimal places
        - ctr, cr -> percentage string ("1.89%")
    """
    where_clauses = [
        f"date BETWEEN '{start_date:%Y-%m-%d}' AND '{end_date:%Y-%m-%d}'"
    ]

    if medium:
        where_clauses.append(f"medium = '{medium}'")

    if countries:
        if isinstance(countries, str):
            country_list = [countries]
        else:
            country_list = list(countries)

        quoted = ", ".join(f"'{c}'" for c in country_list)
        if exclude_countries:
            where_clauses.append(f"country NOT IN ({quoted})")
        else:
            where_clauses.append(f"country IN ({quoted})")

    where_sql = " AND ".join(where_clauses)

    sql = f"""
    SELECT
      SUM(impressions)                                        AS impressions,
      SUM(clicks)                                             AS clicks,
      SAFE_DIVIDE(SUM(clicks), SUM(impressions))              AS ctr,
      SUM(transactions)                                       AS orders,
      SAFE_DIVIDE(SUM(transactions), SUM(clicks))             AS cr,
      SUM(conversion_value)                                   AS turnover,
      SUM(cost)                                               AS spend,
      SAFE_DIVIDE(SUM(conversion_value), SUM(transactions))   AS aov,
      SAFE_DIVIDE(SUM(cost), SUM(transactions))               AS cpo,
      SAFE_DIVIDE(SUM(conversion_value), SUM(cost))           AS roas
    FROM {fqn('ads_online')}
    WHERE {where_sql}
    """
    rows = run_query(sql)
    if not rows:
        return {k: None for k in [
            "impressions", "clicks", "ctr", "orders", "cr",
            "turnover", "spend", "aov", "cpo", "roas",
        ]}

    row = rows[0]
    return {
        "impressions": _round_opt(row.get("impressions")),
        "clicks": _round_opt(row.get("clicks")),
        "ctr": _pct_str(row.get("ctr")),
        "orders": _round_opt(row.get("orders")),
        "cr": _pct_str(row.get("cr")),
        "turnover": _round_opt(row.get("turnover")),
        "spend": _round_opt(row.get("spend")),
        "aov": _round_opt(row.get("aov")),
        "cpo": _round_opt(row.get("cpo")),
        "roas": _round_opt(row.get("roas"), ndigits=2),
    }
