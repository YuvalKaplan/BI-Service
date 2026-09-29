"""
Runs the valuation pass by hand: stores the validated price and market cap, for the latest
completed trading day, of every ticker in use — the tickers in the ETFs' recent holdings, and the
registered lines of the screen stored for that date (after scripts/current_screener.py).

Usage:
    python scripts/current_ticker_values.py --dev
"""
import atexit
from modules.object.exit import cleanup
from modules.ticker import valuation

atexit.register(cleanup)

if __name__ == '__main__':
    stats = valuation.run()
    print(valuation.summary(stats))
