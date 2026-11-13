"""
The market split (modules/cron/market_split.py) from the latest benchmark snapshots: recalculated
only when none is stored for the snapshot's quarter, unless --force.

    python scripts/current_market_split.py [--force] [--prod]
"""
import atexit
import sys
from modules.object.exit import cleanup
from modules.cron import market_split

atexit.register(cleanup)

if __name__ == '__main__':
    stats = market_split.refresh(force='--force' in sys.argv)
    print(market_split.summary(stats))
