"""
Prepares ticker data needed before a historical simulation: refreshes stale ticker profile data
(cik/isin/name/etc., needed for master-ticker grouping), then elects/freezes master tickers and
refreshes accumulated_market_cap. Follow with scripts/sim_benchmark.py (historical benchmark
backfill), then sim_fund.py.

Ticker profile refresh and master sync are idempotent, so it's safe to re-run this even if the
live pipeline has already populated them — it'll just pick up anything new/stale since last time.

Writes a combined report (ticker profile refresh summary, master ticker groups) to
.downloads/sim_prep_data_report.md.

Usage:
    python scripts/sim_prep_data.py --dev
    python scripts/sim_prep_data.py --prod
"""
import atexit
import os
from datetime import datetime
from modules.object.exit import cleanup
from modules.ticker import master

atexit.register(cleanup)

REPORT_PATH = os.path.join(os.path.dirname(__file__), '..', '.downloads', 'sim_prep_data_report.md')

if __name__ == '__main__':
    try:
        include_invalid = input("Also retry tickers previously marked invalid? [y/N] ").strip().lower() == 'y'

        report_sections = ["\n".join([
            "# Sim Data Prep Report",
            "",
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        ])]

        total, updated, marked_invalid = master.refresh_ticker_profiles(include_invalid=include_invalid)
        print(f"Ticker profile refresh: {updated} updated, {marked_invalid} marked invalid, out of {total} checked.")
        report_sections.append("\n".join([
            "", "## Ticker Profile Refresh", "",
            f"Checked: {total} | Updated: {updated} | Marked invalid: {marked_invalid}",
        ]))

        masters_updated, unlinked, caps_updated = master.sync_masters_and_accumulated_caps()
        print(f"Master sync: {masters_updated} link(s), {unlinked} unlinked, {caps_updated} accumulated cap(s) refreshed.")
        groups_report = master.build_master_groups_report(masters_updated, caps_updated, unlinked)
        print(f"\n{groups_report}")
        report_sections.append(groups_report)

        report = "\n\n---\n".join(report_sections)
        os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
        with open(REPORT_PATH, 'w', encoding='utf-8') as f:
            f.write(report)
        print(f"\nReport written to: {REPORT_PATH}")

    except Exception as e:
        print(f"Error in sim data prep: {e}")
