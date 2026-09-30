"""
Downloads the provider ETFs' holdings from FMP and applies the selection rules, as the Tuesday to
Saturday cron does (modules/cron/etf_downloader.py): every active and pending ETF, and those
failing only the freshness rule; each line resolved to a ticker (new ones registered) and stored
under FMP's holdings date.

--retry-unresolved tries again the lines left unresolved before (the cron does on Wednesdays) -
use it after the first run on a new database.

Usage:
    python scripts/current_etf_holdings.py --dev
    python scripts/current_etf_holdings.py --dev --retry-unresolved
"""
import atexit
import sys
from modules.object.exit import cleanup
from modules.cron import etf_downloader

atexit.register(cleanup)

if __name__ == '__main__':
    stats = etf_downloader.run(retry_unresolved=True if '--retry-unresolved' in sys.argv else None)
    print(etf_downloader.summary(stats))
