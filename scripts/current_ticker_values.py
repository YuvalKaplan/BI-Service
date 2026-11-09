"""
Runs the valuation pass by hand: stores the validated price and market cap, for the latest
completed trading day, of every ticker in use — the tickers in the ETFs' recent holdings, and the
registered lines of the screen stored for that date (after scripts/current_screener.py).

--revalidate first clears the invalid flag of every ticker flagged on a price / market-cap
mismatch with FMP's history (pricing.MISMATCH_REASON), so the pass checks them again: a
revision by FMP, or a stored history on another basis or share count, is now taken
(pricing.mismatch_kind), any other mismatch flags the ticker again at once. The cron does the
same on the generation day.

Usage:
    python scripts/current_ticker_values.py --dev
    python scripts/current_ticker_values.py --dev --revalidate
"""
import atexit
import sys
from modules.object.exit import cleanup
from modules.ticker import valuation

atexit.register(cleanup)

if __name__ == '__main__':
    stats = valuation.run(revalidate='--revalidate' in sys.argv)
    print(valuation.summary(stats))
