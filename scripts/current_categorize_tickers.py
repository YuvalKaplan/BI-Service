import atexit
from modules.object.exit import cleanup
from modules.cron import categorize_downloader
from modules.ticker import style

atexit.register(cleanup)

if __name__ == "__main__":
    categorize_downloader.run()
    style.assign_styles()
    print("Assigned styles to unclassified tickers.")
