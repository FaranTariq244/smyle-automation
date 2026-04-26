"""
Daily Report via BigQuery - No browser/Looker Studio needed.

Queries the ads_online table directly to produce the same KPIs
for Overall, Facebook, and Google Ads that the Looker Studio daily
report extracts via browser automation.

Output is a tab-separated row matching the Google Sheets column layout
(columns C-S) so you can paste it directly into Excel for comparison.

Usage:
    python extract_daily_report_bq.py
    # Then enter a date or press Enter for previous day.
"""

from datetime import datetime, timedelta

from services.bigquery.client import fqn, run_query


def get_date_input():
    """Get date from user or use previous day."""
    print("\nEnter date to extract (DD-MMM-YYYY, e.g., 09-Oct-2025)")
    print("Or press Enter for previous day: ", end='')

    date_input = input().strip()

    if not date_input:
        date_obj = datetime.now() - timedelta(days=1)
        date_str = date_obj.strftime('%d-%b-%Y')
        print(f"Using previous day: {date_str}")
    else:
        try:
            date_obj = datetime.strptime(date_input, '%d-%b-%Y')
            date_str = date_input
        except ValueError:
            try:
                date_obj = datetime.strptime(date_input, '%d-%B-%Y')
                date_str = date_obj.strftime('%d-%b-%Y')
            except ValueError:
                print("Invalid date format. Using previous day instead.")
                date_obj = datetime.now() - timedelta(days=1)
                date_str = date_obj.strftime('%d-%b-%Y')

    return date_obj, date_str


def query_daily_kpis(date_obj, medium=None):
    """
    Query the daily report KPIs from ads_online.

    Args:
        date_obj: datetime for the target date.
        medium: None for Overall, 'Facebook', or 'Google Ads'.

    Returns:
        dict with keys: Conversion %, Spend, ROAS, AOV, Conversions
        (the 5 metrics per segment that go into the sheet)
    """
    date_str = date_obj.strftime('%Y-%m-%d')

    where = f"date = '{date_str}'"
    if medium:
        where += f" AND medium = '{medium}'"

    sql = f"""
    SELECT
      SUM(transactions)                                       AS conversions,
      SUM(cost)                                               AS spend,
      SUM(conversion_value)                                   AS online_revenue,
      SAFE_DIVIDE(SUM(transactions), SUM(clicks))             AS conversion_pct,
      SAFE_DIVIDE(SUM(conversion_value), SUM(transactions))   AS aov,
      SAFE_DIVIDE(SUM(conversion_value), SUM(cost))           AS roas
    FROM {fqn('ads_online')}
    WHERE {where}
    """

    rows = run_query(sql)

    if not rows or rows[0].get('conversions') is None:
        return {
            'Conversion %': 0, 'Spend': 0, 'ROAS': 0,
            'AOV': 0, 'Conversions': 0,
        }

    r = rows[0]

    def _val(key, default=0):
        v = r.get(key)
        return float(v) if v is not None else default

    return {
        'Conversion %': round(_val('conversion_pct') * 100, 2),
        'Spend': round(_val('spend'), 2),
        'ROAS': round(_val('roas'), 2),
        'AOV': round(_val('aov'), 2),
        'Conversions': int(_val('conversions')),
    }


def main():
    print("=" * 80)
    print("DAILY REPORT - BIGQUERY (no browser needed)".center(80))
    print("=" * 80)

    date_obj, date_str = get_date_input()

    print(f"\nQuerying BigQuery for {date_str}...")

    results = {}
    for label, medium in [('Overall', None), ('Facebook', 'Facebook'), ('Google Ads', 'Google Ads')]:
        print(f"  Fetching {label}...")
        results[label] = query_daily_kpis(date_obj, medium)

    # Build the tab-separated row matching sheet columns C-S
    full_date = date_obj.strftime("%d/%m/%Y")
    day = date_obj.day

    o = results['Overall']
    fb = results['Facebook']
    g = results['Google Ads']

    # Column order: Date | Day | O.Conv% | O.Spend | O.ROAS | O.AOV | O.Conv
    #                           | FB.Conv% | FB.Spend | FB.ROAS | FB.AOV | FB.Conv
    #                           | G.Conv% | G.Spend | G.ROAS | G.AOV | G.Conv
    cells = [
        full_date,                          # C - Date
        str(day),                           # D - Day
        str(o['Conversion %']),             # E - Overall Conv%
        str(o['Spend']),                    # F - Overall Spend
        str(o['ROAS']),                     # G - Overall ROAS
        str(o['AOV']),                      # H - Overall AOV
        str(o['Conversions']),              # I - Overall Conversions
        str(fb['Conversion %']),            # J - Facebook Conv%
        str(fb['Spend']),                   # K - Facebook Spend
        str(fb['ROAS']),                    # L - Facebook ROAS
        str(fb['AOV']),                     # M - Facebook AOV
        str(fb['Conversions']),             # N - Facebook Conversions
        str(g['Conversion %']),             # O - Google Conv%
        str(g['Spend']),                    # P - Google Spend
        str(g['ROAS']),                     # Q - Google ROAS
        str(g['AOV']),                      # R - Google AOV
        str(g['Conversions']),              # S - Google Conversions
    ]

    row_line = "\t".join(cells)

    # Show the header and row
    print("\n" + "=" * 80)
    print("COPY THE ROW BELOW AND PASTE INTO EXCEL (columns C-S)")
    print("=" * 80)

    header = [
        "Date", "Day",
        "Conv%", "Spend", "ROAS", "AOV", "Conv",       # Overall
        "Conv%", "Spend", "ROAS", "AOV", "Conv",       # Facebook
        "Conv%", "Spend", "ROAS", "AOV", "Conv",       # Google
    ]
    print("\t".join(header))
    print(row_line)

    print("\n" + "=" * 80)

    # Also show a readable breakdown for quick review
    print("\nReadable breakdown:")
    for source in ['Overall', 'Facebook', 'Google Ads']:
        m = results[source]
        print(f"  {source}: Conv%={m['Conversion %']}%, "
              f"Spend={m['Spend']}, ROAS={m['ROAS']}, "
              f"AOV={m['AOV']}, Conv={m['Conversions']}")

    print("\nDone.")


if __name__ == "__main__":
    main()
