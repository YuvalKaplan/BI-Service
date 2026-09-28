"""
Runs the universe screener by hand: registers the FMP large-cap screen's home-market listings
and stores the screen for the latest completed trading day (the Wednesday mode — follow with
scripts/current_ticker_values.py to value it). With --register-only, only registers the listings
as tickers (the other days' mode).

Usage:
    python scripts/current_universe_screener.py --dev
    python scripts/current_universe_screener.py --dev --register-only
"""
import atexit
import sys
from modules.object.exit import cleanup
from modules.cron import universe_screener

atexit.register(cleanup)

if __name__ == '__main__':
    stats = universe_screener.run(store='--register-only' not in sys.argv)
    print(universe_screener.summary(stats))
