import log
import time
import math
import re
from datetime import datetime
from modules.object import batch_run
from modules.core.fdata import get_stock_profile
from modules.object.provider_etf_holding import fetch_valid_tickers_in_holdings
from modules.object import ticker, ticker_value

REMOVE_ETFS_AND_FUNDS = r'\b(ETF|fund)\b'
BATCH_SIZE = 100

def run() -> tuple[int, int]:
    try:
        batch_run_id = None
        if batch_run_id is None:
            batch_run_id = batch_run.insert(batch_run.BatchRun('stock_downloader', 'auto'))

        symbols = fetch_valid_tickers_in_holdings()
        group_count = 1
        today = datetime.today()
        missing_symbols = []
        start = time.monotonic()
        num_groups = math.ceil(len(symbols) / BATCH_SIZE)

        log.record_status(f"Starting Stock Info Download in {num_groups} batches of {BATCH_SIZE} stocks each.")

        for i in range(0, len(symbols), BATCH_SIZE):
            batch = symbols[i : i + BATCH_SIZE]
            for s in batch:
                sd = get_stock_profile(s)
                if isinstance(sd, str):
                    ticker.update_invalid(s, sd)
                    missing_symbols.append(s)
                else:
                    t = ticker.Ticker(symbol=s, isin=sd['isin'], cik=sd['cik'], exchange=sd['exchange'], name=sd['companyName'], industry=sd['industry'], sector=sd['sector'])
                    if t.name is None or re.search(REMOVE_ETFS_AND_FUNDS, t.name, flags=re.IGNORECASE):
                        ticker.update_invalid(s, 'Fund or ETF')
                        missing_symbols.append(s)
                    else:    
                        ticker.update_info(t)
                        tv = ticker_value.TickerValue(symbol=s, value_date=today, stock_price=float(sd['price']), market_cap=float(sd['marketCap']))
                        ticker_value.upsert(tv)

            log.record_status(f"Batch {group_count} of {num_groups} completed.")
            group_count += 1

        log.record_status(f"Run time (seconds): {time.monotonic() - start}")
        log.record_status(f"Missing symbols ({len(missing_symbols)}): {', '.join(missing_symbols)}")
        log.record_status(f"Finished Stock Info Download batch run on {len(symbols)} stocks.")

        batch_run.update_completed_at(batch_run_id)
        return len(symbols), len(missing_symbols)

    except Exception as e:
        log.record_error(f"Error in downloading stock info batch run: {e}")
        raise e