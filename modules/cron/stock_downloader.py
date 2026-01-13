import log
import time
import math
from datetime import datetime
from modules.object import batch_run
from modules.core.fdata import get_stock_profile
from modules.object.provider_etf_holding import fetch_tickers_in_holdings
from modules.object import ticker, ticker_value

def run() -> tuple[int, int]:
    try:
        batch_run_id = None
        if batch_run_id is None:
            batch_run_id = batch_run.insert(batch_run.BatchRun('stock_downloader', 'auto'))

        symbols = fetch_tickers_in_holdings()
        group_size = 100
        group_count = 1
        today = datetime.today()
        missing_symbols = []
        start = time.monotonic()
        num_groups = math.ceil(len(symbols) / group_size)

        print(f"Starting stock info download in {num_groups} batches of {group_size} stocks each.")

        for i in range(0, len(symbols), group_size):
            batch = symbols[i : i + group_size]
            for s in batch:
                # No need for throtteling logic as the call response time is over 1 second and we are allowed 300 per minute.
                sd = get_stock_profile(s)
                if sd:
                    t = ticker.Ticker(symbol=s, isin=sd['isin'], cik=sd['cik'], exchange=sd['exchange'], name=sd['companyName'], industry=sd['industry'], sector=sd['sector'])
                    ticker.update_info(t)
                    tv = ticker_value.TickerValue(symbol=s, value_date=today, stock_price=float(sd['price']), market_cap=float(sd['marketCap']))
                    ticker_value.upsert(tv)
                else:
                    missing_symbols.append(s)

            print(f"Batch {group_count} completed.")
            group_count += 1

        log.record_status(f"Run time (seconds): {time.monotonic() - start}")
        log.record_status(f"Missing symbols ({len(missing_symbols)}): {', '.join(missing_symbols)}")
        log.record_status(f"Finished stock downloader batch run on {len(symbols)} stocks.")

        batch_run.update_completed_at(batch_run_id)
        return len(symbols), len(missing_symbols)

    except Exception as e:
        log.record_error(f"Error in downloading stock info batch run: {e}")
        raise e