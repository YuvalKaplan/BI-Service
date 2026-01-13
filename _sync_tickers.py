import atexit
from modules.init.exit import cleanup
from modules.cron.categorize_tickers import run

atexit.register(cleanup)

if __name__ == '__main__':
    try:
        total_symbols = run()
        print(f"Categorized tickers: {total_symbols}")

    except Exception as e:
        print(f"Error in ticker categorization: {e}")
