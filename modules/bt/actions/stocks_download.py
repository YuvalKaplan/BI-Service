import log
import time
import math
import re
from decimal import Decimal
from typing import List
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor
from modules.core.api_stocks import get_stock_profile, get_stock_historic_prices, get_stock_historic_dividend, get_stock_historic_splits, get_stock_historic_market_cap, fetch_esg_data
from modules.calc import esg

from modules.bt.object import account, ticker_split_history
from modules.bt.object import fund
from modules.bt.object.provider_etf_holding import fetch_tickers_for_etfs
from modules.bt.object import ticker, ticker_value, ticker_dividend_history

REMOVE_ETFS_AND_FUNDS = r'\b(ETF|fund)\b'

def parse_date(d: str | date) -> date:
    if isinstance(d, str):
        return date.fromisoformat(d)
    elif isinstance(d, date):
        return d
    else:
        raise ValueError(f"Cannot parse date: {d} of type {type(d)}")

def process_symbol(s: str, start_date: date, end_date: date) -> tuple[bool, str, str | None]:
    try:
        t = ticker.fetch_by_symbol(s)
        if t is not None and t.invalid:
            return False, s, "Invalid ticker"

        # Phase 1: fetch profile and validate before downloading anything else
        sd = get_stock_profile(s)
        if isinstance(sd, str):
            ticker.update_invalid(s, sd)
            return False, s, sd

        t = ticker.Ticker(
            symbol=s,
            isin=sd['isin'],
            cik=sd['cik'],
            exchange=sd['exchange'],
            name=sd['companyName'],
            industry=sd['industry'],
            sector=sd['sector'],
            source='provider_etf'
        )

        ticker.upsert(t)

        if t.name is None:
            ticker.update_invalid(s, 'Missing details')
            return False, s, 'Missing details'

        if re.search(REMOVE_ETFS_AND_FUNDS, t.name, flags=re.IGNORECASE):
            ticker.update_invalid(s, 'Fund or ETF')
            return False, s, 'Fund or ETF'

        disclosure, rating = fetch_esg_data(s)
        esg_qualified, esg_factors = esg.qualify(disclosure, rating)
        ticker.update_esg_data(s, esg_qualified, esg_factors)

        # Phase 2: fetch historical data in parallel now that the ticker is valid
        
        # Check if sufficient data already exists
        available_dates = ticker_value.fetch_tickers_availability_dates(s, start_date, end_date)
        if available_dates:
            available_set = set()
            for row in available_dates:
                try:
                    d = parse_date(row['value_date'])
                    available_set.add(d)
                except ValueError as e:
                    log.record_error(f"Skipping invalid date for {s}: {e}")
                    continue
            # Filter to date range and weekdays
            range_dates = [d for d in available_set if start_date <= d <= end_date and d.weekday() < 5]
            expected_days = weekday_count(start_date, end_date)
            coverage = len(range_dates) / expected_days if expected_days else 0
            if coverage >= 0.85:
                return True, s, None  # Sufficient data already exists

        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {
                'prices':    executor.submit(get_stock_historic_prices, s, start_date, end_date),
                'market_cap': executor.submit(get_stock_historic_market_cap, s, start_date, end_date),
                'dividends': executor.submit(get_stock_historic_dividend, s),
                'splits':    executor.submit(get_stock_historic_splits, s),
            }
            results = {name: future.result() for name, future in futures.items()}

        shp = results['prices']
        if isinstance(shp, str):
            ticker.update_invalid(s, shp)
            return False, s, shp

        shmc = results['market_cap']
        if isinstance(shmc, str):
            ticker.update_invalid(s, shmc)
            return False, s, shmc
        
        shd = results['dividends']
        if isinstance(shd, str):
            ticker.update_invalid(s, shd)
            return False, s, shd

        shs = results['splits']
        if isinstance(shs, str):
            ticker.update_invalid(s, shs)
            return False, s, shs

        # Process dividends
        dividends = [
            ticker_dividend_history.TickerDividendHistory(
                symbol=s,
                ex_date=parse_date(line['date']),
                amount_per_share=Decimal(line['dividend'])
            )
            for line in shd
        ]
        ticker_dividend_history.insert_dividends_bulk(dividends)

        # Process splits
        splits = [
            ticker_split_history.TickerSplitHistory(
                symbol=s,
                date=parse_date(line['date']),
                numerator=Decimal(line['numerator']),
                denominator=Decimal(line['denominator'])
            )
            for line in shs
        ]
        ticker_split_history.insert_split_bulk(splits)

        # combine all days data and save
        price_by_date = {parse_date(row["date"]): float(row["price"]) for row in shp}
        mc_by_date = {parse_date(row["date"]): float(row["marketCap"]) for row in shmc}

        # Intersection of dates that have both values
        common_dates = price_by_date.keys() & mc_by_date.keys()

        # Keep only weekdays
        common_weekdays = [d for d in common_dates if d.weekday() < 5]

        expected_days = weekday_count(min(common_dates), max(common_dates))

        valid_days = len(common_weekdays)
        coverage = valid_days / expected_days if expected_days else 0

        if coverage < 0.85:
            reason = f"Insufficient data coverage ({coverage:.1%})"
            ticker.update_invalid(s, reason)
            return False, s, reason

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

        return True, s, None

    except Exception as e:
        log.record_error(f"Error processing symbol {s}: {e}")
        return False, s, str(e)

def weekday_count(start: date, end: date) -> int:
    days = (end - start).days + 1
    weeks, remainder = divmod(days, 7)
    count = weeks * 5
    for i in range(remainder):
        if (start + timedelta(days=weeks * 7 + i)).weekday() < 5:
            count += 1
    return count

def run(symbols: list[str], start_date: date, end_date: date) -> tuple[int, int, int]:
    try:
        missing_symbols = []
        existing = 0
        start = time.monotonic()
        count = 0

        print(f"Starting Stock Info Download for {len(symbols)} ticker symbols.")
        
        # Use ThreadPoolExecutor for parallel processing
        with ThreadPoolExecutor(max_workers=5) as executor:  # Reduced to 5 to ease API rate limiting
            futures = [executor.submit(process_symbol, s, start_date, end_date) for s in symbols]
            
            for future in futures:
                success, symbol, reason = future.result()
                if success:
                    count += 1
                    print(f"Downloaded and saved {symbol} ({count} out of {len(symbols)})")
                else:
                    missing_symbols.append(symbol)

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