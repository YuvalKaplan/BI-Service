import atexit
from modules.init.exit import cleanup
from modules.core.fdata import get_stock_profile
from modules.object.provider_etf_holding import fetch_tickers_in_holdings
from modules.object.ticker import Ticker, update_info

atexit.register(cleanup)

if __name__ == '__main__':
    try:
        symbols = fetch_tickers_in_holdings()
        batch_size = 100

        all_market_cap_data = []
        missing_symbols = []

        for i in range(0, len(symbols), batch_size):
            batch = symbols[i : i + batch_size]
            for s in batch:
                sd = get_stock_profile(s)
                if sd:
                    t = Ticker(symbol==s, isin=sd.isin)
                    update_info(t)

        print(f"Total Symbols Processed: {len(symbols)}")
        print(f"Data retrieved for: {len(all_market_cap_data)} symbols")
        print(f"Missing symbols ({len(missing_symbols)}): {', '.join(missing_symbols)}")

    except Exception as e:
        print(f"Error in scraping and processing categorizer ETFs: {e}")
