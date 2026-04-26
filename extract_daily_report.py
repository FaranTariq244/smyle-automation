"""
Daily Report Extraction via BigQuery - No browser needed.

Queries ads_online directly for Overall, Facebook, and Google Ads KPIs,
then saves to Google Sheets in the same format as before.
"""

from datetime import datetime, timedelta
from extract_daily_report_bq import get_date_input, query_daily_kpis


def display_results(date_str, results):
    """Display results in clean format."""
    print("\n" + "=" * 80)
    print(f"REPORT DATA FOR {date_str}".center(80))
    print("=" * 80)

    for source in ['Overall', 'Facebook', 'Google Ads']:
        metrics = results[source]
        print(f"\n{source.upper()}")
        print("-" * 40)
        print(f"Conversion %:     {metrics.get('Conversion %', 0):>14.2f}%")
        print(f"Spend:            \u20ac{metrics.get('Spend', 0):>14,.2f}")
        print(f"ROAS:             {metrics.get('ROAS', 0):>15.2f}")
        print(f"AOV:              \u20ac{metrics.get('AOV', 0):>14,.2f}")
        print(f"Conversions:      {metrics.get('Conversions', 0):>15,.0f}")

    print("\n" + "=" * 80)


def save_to_google_sheets(date_obj, results):
    """Save results to Google Sheets using service account."""
    try:
        from dotenv import load_dotenv
        from services.sheets.helpers import write_marketing_data

        load_dotenv()

        print("\nSaving to Google Sheets...")
        write_marketing_data(
            date_obj,
            results['Overall'],
            results['Facebook'],
            results['Google Ads']
        )
        print("  ✓ Successfully saved to Google Sheets!")

    except Exception as e:
        print(f"\n  ✗ Failed to save to Google Sheets: {e}")
        import traceback
        traceback.print_exc()


def main():
    print("=" * 80)
    print("DAILY REPORT EXTRACTION (BigQuery)".center(80))
    print("=" * 80)

    date_obj, date_str = get_date_input()

    print(f"\nQuerying BigQuery for {date_str}...")

    results = {}
    for label, medium in [('Overall', None), ('Facebook', 'Facebook'), ('Google Ads', 'Google Ads')]:
        print(f"  Fetching {label}...")
        results[label] = query_daily_kpis(date_obj, medium)

    display_results(date_str, results)
    save_to_google_sheets(date_obj, results)

    print("\nExtraction complete!")


if __name__ == "__main__":
    main()
