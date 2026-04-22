import atexit
from datetime import date, timedelta
from modules.object.exit import cleanup
from modules.object.provider_etf_holding import fetch_valid_ticker_ids_in_holdings
from modules.object.ticker import fetch_by_ids
from modules.object.ticker_value import TickerValue, upsert_bulk
from modules.ticker.resolver import get_full_symbol
from modules.core.api_stocks import get_stock_historic_prices, get_stock_historic_market_cap
from modules.core.db import db_pool_instance
from psycopg.errors import Error

atexit.register(cleanup)

FILL_START_DATE = date(2026, 1, 1)
MIN_GAP_DAYS = 4


def fetch_existing_dates(ticker_id: int, start: date, end: date) -> set[date]:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT value_date
                    FROM ticker_value
                    WHERE ticker_id = %s
                      AND value_date >= %s
                      AND value_date <= %s
                """, (ticker_id, start, end))
                return {row[0] for row in cur.fetchall()}
    except Error as e:
        raise Exception(f"Error fetching existing dates for ticker_id {ticker_id}: {e}")


def find_gaps(existing: set[date], start: date, end: date) -> list[tuple[date, date]]:
    """Return list of (gap_start, gap_end) spans of missing weekdays longer than MIN_GAP_DAYS."""
    gaps = []
    gap_start = None

    current = start
    while current <= end:
        if current.weekday() < 5:  # weekday only
            is_missing = current not in existing
            if is_missing and gap_start is None:
                gap_start = current
            elif not is_missing and gap_start is not None:
                gap_end = current - timedelta(days=1)
                if (gap_end - gap_start).days >= MIN_GAP_DAYS:
                    gaps.append((gap_start, gap_end))
                gap_start = None
        current += timedelta(days=1)

    if gap_start is not None:
        gap_end = end
        if (gap_end - gap_start).days >= MIN_GAP_DAYS:
            gaps.append((gap_start, gap_end))

    return gaps


def fill_gap(ticker_id: int, fmp_symbol: str, gap_start: date, gap_end: date) -> int:
    prices_raw = get_stock_historic_prices(fmp_symbol, gap_start, gap_end)
    if isinstance(prices_raw, str):
        print(f"  [{fmp_symbol}] prices unavailable for {gap_start}–{gap_end}: {prices_raw}")
        return 0

    mc_raw = get_stock_historic_market_cap(fmp_symbol, gap_start, gap_end)
    if isinstance(mc_raw, str):
        print(f"  [{fmp_symbol}] market cap unavailable for {gap_start}–{gap_end}: {mc_raw}")
        return 0

    price_by_date = {date.fromisoformat(row["date"]): float(row["price"]) for row in prices_raw}
    mc_by_date = {date.fromisoformat(row["date"]): float(row["marketCap"]) for row in mc_raw}

    common_dates = price_by_date.keys() & mc_by_date.keys()
    items = [
        TickerValue(ticker_id=ticker_id, value_date=d, stock_price=price_by_date[d], market_cap=mc_by_date[d])
        for d in common_dates
        if d.weekday() < 5
    ]

    if items:
        upsert_bulk(items)

    return len(items)


if __name__ == "__main__":
    end_date = date.today()
    ticker_ids = fetch_valid_ticker_ids_in_holdings()
    tickers = fetch_by_ids(ticker_ids)
    print(f"Checking {len(tickers)} tickers for gaps between {FILL_START_DATE} and {end_date} (min gap: {MIN_GAP_DAYS} days).\n")

    total_filled = 0
    tickers_filled = 0

    for ticker in tickers:
        assert ticker.id is not None

        existing = fetch_existing_dates(ticker.id, FILL_START_DATE, end_date)
        gaps = find_gaps(existing, FILL_START_DATE, end_date)

        if not gaps:
            continue

        full_symbol = get_full_symbol(ticker)

        print(f"[{full_symbol}] {len(gaps)} gap(s) found:")
        ticker_filled = 0
        for gap_start, gap_end in gaps:
            print(f"  {gap_start} → {gap_end} ({(gap_end - gap_start).days + 1} days)")
            filled = fill_gap(ticker.id, full_symbol, gap_start, gap_end)
            ticker_filled += filled
            print(f"  Filled {filled} rows.")

        if ticker_filled > 0:
            total_filled += ticker_filled
            tickers_filled += 1

    print(f"\nDone. Filled {total_filled} rows across {tickers_filled} tickers.")
