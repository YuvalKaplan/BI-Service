import atexit
import time
import atexit
from modules.init.exit import cleanup
from modules.cron.stock_downloader import run

atexit.register(cleanup)

if __name__ == '__main__':
    try:
        total_updated, missing_data = run()
        print(f"Stocks updated: {total_updated}")
        print(f"Stocks missing data: {missing_data}")

    except Exception as e:
        print(f"Error in downloadinf stock price and market cap: {e}")
