"""
Runs the valuation pass by hand: stores the validated price and market cap, for the latest
completed trading day, of every ticker in use — the tickers in the ETFs' recent holdings, and the
registered lines of the screen stored for that date (after scripts/current_screener.py).

--revalidate first clears the invalid flag of every ticker flagged on a price / market-cap
mismatch with FMP's history (pricing.MISMATCH_REASON), so the pass checks them again: a
revision by FMP or a stored history on another basis is now taken (pricing.mismatch_kind), any
other mismatch flags the ticker again at once.

Usage:
    python scripts/current_ticker_values.py --dev
    python scripts/current_ticker_values.py --dev --revalidate
"""
import atexit
import sys
from modules.object import ticker
from modules.object.exit import cleanup
from modules.ticker import pricing, valuation

atexit.register(cleanup)

if __name__ == '__main__':
    if '--revalidate' in sys.argv:
        cleared = ticker.clear_invalid_with_prefix(pricing.MISMATCH_REASON)
        print(f"{cleared} ticker(s) flagged on a mismatch with FMP's history cleared, to be checked again.")
    stats = valuation.run()
    print(valuation.summary(stats))
