import log
import time
import math
import re
from decimal import Decimal
from typing import List
from datetime import date, timedelta
from modules.core.fdata import get_stock_profile, get_stock_historic_prices, get_stock_historic_dividend, get_stock_historic_market_cap

from modules.bt.object import account
from modules.bt.object import fund
from modules.bt.object.provider_etf_holding import fetch_tickers_for_etfs
from modules.bt.object import ticker, ticker_value, ticker_dividend_history

REMOVE_ETFS_AND_FUNDS = r'\b(ETF|fund)\b'

def weekday_count(start: date, end: date) -> int:
    days = (end - start).days + 1
    weeks, remainder = divmod(days, 7)
    count = weeks * 5
    for i in range(remainder):
        if (start + timedelta(days=weeks * 7 + i)).weekday() < 5:
            count += 1
    return count

def run(etf_ids: list[int], start_date: date, end_date: date) -> tuple[int, int, int]:
    try:
        symbols = fetch_tickers_for_etfs(etf_ids)
        missing_symbols = []
        existing = 0
        start = time.monotonic()

        print(f"Starting Stock Info Download for {len(symbols)} ticker symbols.")
        
        for s in symbols:
            t = ticker.fetch_by_symbol(s)
            if t is not None and t.invalid:
                missing_symbols.append(s)
                continue

            # See if already loaded
            avialable = ticker_value.fetch_tickers_availability_dates(s)
            if len(avialable) != 0:
                existing += 1
                continue

            sd = get_stock_profile(s)
            if isinstance(sd, str):
                ticker.update_invalid(s, sd)
                missing_symbols.append(s)
                continue

            t = ticker.Ticker(symbol=s, isin=sd['isin'], cik=sd['cik'], exchange=sd['exchange'], name=sd['companyName'], industry=sd['industry'], sector=sd['sector'])
            if t.name is None or re.search(REMOVE_ETFS_AND_FUNDS, t.name, flags=re.IGNORECASE):
                ticker.update_invalid(s, 'Fund or ETF')
                missing_symbols.append(s)
                continue

            ticker.upsert(t) 

            shd: list[dict[str,str]] | str = get_stock_historic_dividend(s)
            if isinstance(shd, str):
                ticker.update_invalid(s, shd)
                missing_symbols.append(s)
                continue

            dividends = [
                ticker_dividend_history.TickerDividendHistory(
                    symbol=s,
                    ex_date=date.fromisoformat(line['date']),
                    amount_per_share=Decimal(line['dividend'])
                )
                for line in shd
            ]
            ticker_dividend_history.insert_dividends_bulk(dividends)
 

            shp = get_stock_historic_prices(s, start_date, end_date)
            if isinstance(shp, str):
                ticker.update_invalid(s, shp)
                missing_symbols.append(s)
                continue

            shmc = get_stock_historic_market_cap(s, start_date, end_date)
            if isinstance(shmc, str):
                ticker.update_invalid(s, shmc)
                missing_symbols.append(s)
                continue

            # combile all days data and save
            price_by_date = {date.fromisoformat(row["date"]): float(row["price"]) for row in shp}
            mc_by_date = {date.fromisoformat(row["date"]): float(row["marketCap"]) for row in shmc}

            # Intersection of dates that have both values
            common_dates = price_by_date.keys() & mc_by_date.keys()

            # Keep only weekdays
            common_weekdays = [d for d in common_dates if d.weekday() < 5]

            expected_days = weekday_count(start_date, end_date)

            valid_days = len(common_weekdays)
            coverage = valid_days / expected_days if expected_days else 0

            if coverage < 0.85:
                ticker.update_invalid(s, f"Insufficient data coverage ({coverage:.1%})")
                missing_symbols.append(s)
                continue

            # Build dataclass list (vectorized style)
            ticker_values = [
                ticker_value.TickerValue(
                    symbol=s,
                    value_date=d,
                    stock_price=price_by_date[d],
                    market_cap=mc_by_date[d],
                )
                for d in common_weekdays
            ]

            # BULK insert instead of row-by-row
            ticker_value.upsert_bulk(ticker_values)

            print(f"Downloaded and saved {s}")

        log.record_status(f"Run time (seconds): {time.monotonic() - start}")
        if existing:
            log.record_status(f"Existing from previous runs: {existing}")
        
        if missing_symbols:
            log.record_status(f"Missing symbols ({len(missing_symbols)}): {', '.join(missing_symbols)}")

        log.record_status(f"Finished Stock Info Download on {len(symbols)} stocks.")

        return len(symbols), existing, len(missing_symbols)

    except Exception as e:
        log.record_error(f"Error in downloading stock info run: {e}")
        raise e