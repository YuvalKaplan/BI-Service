"""
Exports how a fund's holdings were derived on a recalculation date (its inception date by
default) into .output/methodology/<fund name>_<fund_id>_<date>/:

    README.md          strategy, constants and step-by-step methodology
    etfs/<etf>_<id>.xlsx   each constituent ETF's holdings and active-weight calculation
    benchmark.xlsx     the benchmark(s) the ETFs were compared against
    best_ideas.xlsx    every ETF's best ideas and whether they passed the fund's filters
    fund.xlsx          the fund's holdings with their justification, and the unselected candidates

Read-only. Needs the fund_analysis snapshot for that date, which funds_update writes on every
fund recalculation (live and scripts/sim_fund.py).

Usage:
    python scripts/report_fund_methodology.py --dev
    python scripts/report_fund_methodology.py --prod
"""
import atexit
from datetime import date
from modules.object.exit import cleanup
from modules.report import fund_methodology

atexit.register(cleanup)

if __name__ == '__main__':
    try:
        fund_id = 8  # <-- edit before each run
        as_of_date: date | None = None  # <-- edit before each run (None = the fund's inception date)

        out_dir = fund_methodology.run(fund_id, as_of_date)
        print(f"\nMethodology written to: {out_dir}")

    except Exception as e:
        print(f"Error in fund methodology report: {e}")
