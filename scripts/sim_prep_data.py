"""
Prepares all data needed before running a historical simulation (sim_fund.py): refreshes
stale ticker profile data (cik/isin/name/etc., needed for master-ticker grouping), elects/
freezes master tickers and refreshes accumulated_market_cap, then backfills historical
benchmark_holding snapshots (what sim_benchmark.py used to do on its own). Combined into one
script so sim data prep is a single call instead of three.

Ticker profile refresh and master sync are idempotent, so it's safe to re-run this even if the
live pipeline has already populated them — it'll just pick up anything new/stale since last time.

Writes a combined report (ticker profile refresh summary, master ticker groups, benchmark
snapshots created) to .downloads/sim_prep_data_report.md.

Usage:
    python scripts/sim_prep_data.py --dev
    python scripts/sim_prep_data.py --prod
"""
import atexit
import os
from datetime import date, datetime
from modules.object.exit import cleanup
from modules.ticker import master
from modules.sim import benchmark_generator

atexit.register(cleanup)

REPORT_PATH = os.path.join(os.path.dirname(__file__), '..', '.downloads', 'sim_prep_data_report.md')

if __name__ == '__main__':
    try:
        inception_date = date(2026, 6, 1)  # <-- edit before each run

        include_invalid = input("Also retry tickers previously marked invalid? [y/N] ").strip().lower() == 'y'

        report_sections = ["\n".join([
            "# Sim Data Prep Report",
            "",
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"Inception date: {inception_date}",
        ])]

        total, updated, marked_invalid = master.refresh_ticker_profiles(include_invalid=include_invalid)
        print(f"Ticker profile refresh: {updated} updated, {marked_invalid} marked invalid, out of {total} checked.")
        report_sections.append("\n".join([
            "", "## Ticker Profile Refresh", "",
            f"Checked: {total} | Updated: {updated} | Marked invalid: {marked_invalid}",
        ]))

        masters_updated, caps_updated = master.sync_masters_and_accumulated_caps()
        print(f"Master sync: {masters_updated} link(s), {caps_updated} accumulated cap(s) refreshed.")
        groups_report = master.build_master_groups_report(masters_updated, caps_updated)
        print(f"\n{groups_report}")
        report_sections.append(groups_report)

        benchmarks_created = benchmark_generator.run(inception_date)
        benchmarks_lines = [
            "", "## Benchmarks Created", "",
            f"Total snapshots: {len(benchmarks_created)}", "",
        ] + [
            f"- {name} — {holding_date}: {num_holdings} holdings"
            for name, holding_date, num_holdings in benchmarks_created
        ]
        report_sections.append("\n".join(benchmarks_lines))

        report = "\n\n---\n".join(report_sections)
        os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
        with open(REPORT_PATH, 'w', encoding='utf-8') as f:
            f.write(report)
        print(f"\nReport written to: {REPORT_PATH}")

    except Exception as e:
        print(f"Error in sim data prep: {e}")
