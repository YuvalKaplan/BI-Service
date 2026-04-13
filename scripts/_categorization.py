import atexit
from modules.object.exit import cleanup
from modules.cron import categorize_tickers

atexit.register(cleanup)

if __name__ == '__main__':
    try:
        total_etfs = categorize_tickers.download_data()
        print(f"ETFs processed: {total_etfs}")

        categorize_tickers.run_classification()
        print(f"Tickers classified")

    except Exception as e:
        print(f"Error in categorization: {e}")
