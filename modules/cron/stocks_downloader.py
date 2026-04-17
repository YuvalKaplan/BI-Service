import log
import time
import math
import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from modules.object import batch_run
from modules.core.api_stocks import get_stock_profile
from modules.object.provider_etf_holding import fetch_valid_tickers_in_holdings
from modules.object import ticker, ticker_value

REMOVE_ETFS_AND_FUNDS = r'\b(ETF|fund)\b'

BATCH_SIZE = 100
VALUE_DATE_CUT_OFF_HOUR = 17

def run() -> tuple[int, int]:
    try:
        batch_run_id = None
        if batch_run_id is None:
            batch_run_id = batch_run.insert(batch_run.BatchRun('stock_downloader', 'auto'))

        ticker.sanitize()
        symbols = fetch_valid_tickers_in_holdings()
        group_count = 1
        now_et = datetime.now(ZoneInfo("America/New_York"))
        today = (now_et - timedelta(days=1) if now_et.hour < VALUE_DATE_CUT_OFF_HOUR else now_et).date()
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
                    t = ticker.Ticker(
                        symbol=s,
                        isin=sd['isin'],
                        cik=sd['cik'],
                        exchange=sd['exchange'],
                        name=sd['companyName'],
                        industry=sd['industry'],
                        sector=sd['sector'],
                        currency=sd.get('currency'),
                        source='provider_etf'
                    )
                    ticker.upsert(t)
                    if t.name is None:
                        ticker.update_invalid(s, 'Missing details')
                        missing_symbols.append(s)
                        continue

                    if re.search(REMOVE_ETFS_AND_FUNDS, t.name, flags=re.IGNORECASE):
                        ticker.update_invalid(s, 'Fund or ETF')
                        missing_symbols.append(s)
                        continue
                        
                    tv = ticker_value.TickerValue(
                        symbol=s, 
                        value_date=today, 
                        stock_price=float(sd['price']), 
                        market_cap=float(sd['marketCap'])
                    )
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