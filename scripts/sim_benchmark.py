"""
Backfills historical Wednesday-dated benchmark_holding snapshots (every enabled benchmark) from
inception_date up to the most recent date with published FMP data, for use by a historical
simulation (sim_fund.py).

Run after scripts/sim_prep_data.py (ticker profile refresh, universe screener, master ticker
sync, large-cap universe) — this forms the benchmarks from the stored universe, but doesn't
screen, link or build it.

Writes a report (every snapshot created, with its date and holdings count) to
.output/sim_benchmark_report.md.

Usage:
    python scripts/sim_benchmark.py --dev
    python scripts/sim_benchmark.py --prod
"""
import atexit
import os
from datetime import date, datetime
from modules.object.exit import cleanup
from modules.sim import benchmark_generator

atexit.register(cleanup)

REPORT_PATH = os.path.join(os.path.dirname(__file__), '..', '.output', 'sim_benchmark_report.md')

if __name__ == '__main__':
    try:
        inception_date = date(2026, 6, 1)  # <-- edit before each run

        screen_date, benchmarks_created = benchmark_generator.run(inception_date)

        report_lines = [
            "# Sim Benchmark Report",
            "",
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"Inception date: {inception_date}",
            f"Universe: screen of {screen_date} (see .output/sim_prep_data_report.md)",
            "",
            f"Total snapshots: {len(benchmarks_created)}",
            "",
        ] + [
            f"- {name} — {holding_date}: {num_holdings} holdings"
            for name, holding_date, num_holdings in benchmarks_created
        ]

        report = "\n".join(report_lines)
        print(f"\n{report}")
        os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
        with open(REPORT_PATH, 'w', encoding='utf-8') as f:
            f.write(report)
        print(f"\nReport written to: {REPORT_PATH}")

    except Exception as e:
        print(f"Error in sim benchmark: {e}")
