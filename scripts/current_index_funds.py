"""
Downloads the index funds' holdings (universe_etf: VTI, VEA, VWO), stores the snapshot with each
line's company float cap (universe_etf_holding) and the market's size breakpoints
(market_breakpoint), as the cron does on the generation day before the free float
(modules/ticker/index_funds.py), and prints the breakpoints with the benchmarks' cutoffs.

Usage:
    python scripts/current_index_funds.py --dev
"""
import atexit
from modules.object.exit import cleanup
from modules.object import benchmark
from modules.ticker import index_funds

atexit.register(cleanup)

SHOWN_COVERAGES = (0.70, 0.80, 0.85, 0.90, 0.93, 0.95)

if __name__ == '__main__':
    stats = index_funds.refresh()
    print(index_funds.summary(stats))
    print(f"\nBreakpoints on {stats.as_of} (float cap at which the largest companies make up the coverage):")
    for market in sorted({m for m, _c in stats.breakpoints}):
        for coverage in SHOWN_COVERAGES:
            cap, n = stats.breakpoints[(market, coverage)]
            print(f"  {market:13s} {coverage:.0%}: ${cap / 1e9:8,.1f}B  ({n:,} companies)")
    print("\nBenchmark cutoffs (whole company cap, at least "
          f"{index_funds.MIN_FLOAT_FACTOR:.0%} floating):")
    for b in benchmark.fetch_all():
        print(f"  {b.name}: {b.market_coverage:.0%} of {b.region} -> ${index_funds.cutoff(b.region, b.market_coverage) / 1e9:,.1f}B")
