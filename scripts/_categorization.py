import atexit
from modules.object.exit import cleanup
from modules.cron import stocks_categorize

atexit.register(cleanup)

if __name__ == '__main__':
    try:
        total_etfs = stocks_categorize.download_data()
        print(f"ETFs processed: {total_etfs}")

    except Exception as e:
        print(f"Error in categorization: {e}")
