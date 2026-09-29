import atexit
from datetime import date, timedelta
from modules.object.exit import cleanup
from modules.object.provider_etf_holding import fetch_valid_ticker_ids_in_holdings
from modules.object.ticker import fetch_by_ids
from modules.object.ticker_value import TickerValue, upsert_bulk
from modules.ticker.resolver import TickerResolver
from modules.ticker.pricing import fetch_price_and_market_cap_history
from modules.ticker.util import listing_currency
from modules.core.db import db_pool_instance
from psycopg.errors import Error

atexit.register(cleanup)

FILL_START_DATE = date(2026, 1, 1)
MIN_GAP_DAYS = 4
GLITCH_CONTEXT_DAYS = 60  # history fetched on each side of a gap for the FMP glitch filter


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


def fill_gap(ticker_id: int, fmp_symbol: str, gap_start: date, gap_end: date, exchange: str | None = None, currency: str | None = None,
             verified_shares: float | None = None) -> int:
    # Same fetch as the live path (USD at each date's own FX rate, from the listing's currency;
    # FMP glitches repaired from the price or dropped), over the gap plus context on both sides
    # so the glitch filter can tell a wrong-unit value from a normal one; only the gap's dates
    # are stored.
    fetched = fetch_price_and_market_cap_history(
        fmp_symbol,
        gap_start - timedelta(days=GLITCH_CONTEXT_DAYS),
        min(gap_end + timedelta(days=GLITCH_CONTEXT_DAYS), date.today()),
        currency=listing_currency(exchange, currency),
        verified_shares=verified_shares,
    )
    if isinstance(fetched, str):
        print(f"  [{fmp_symbol}] history unavailable for {gap_start}–{gap_end}: {fetched}")
        return 0

    items = [
        TickerValue(ticker_id=ticker_id, value_date=d, stock_price=price, market_cap=market_cap)
        for d, (price, market_cap) in fetched.items()
        if gap_start <= d <= gap_end
    ]

    if items:
        upsert_bulk(items)

    return len(items)


if __name__ == "__main__":
    resolver = TickerResolver(TickerResolver.POPULATE_TICKER)
    end_date = date.today()
    ticker_ids = fetch_valid_ticker_ids_in_holdings()
    tickers = fetch_by_ids(ticker_ids)
    print(f"Checking {len(tickers)} tickers for gaps between {FILL_START_DATE} and {end_date} (min gap: {MIN_GAP_DAYS} days).\n")

    total_filled = 0
    tickers_filled = 0

    for ticker in tickers:
        existing = fetch_existing_dates(ticker.id, FILL_START_DATE, end_date)
        gaps = find_gaps(existing, FILL_START_DATE, end_date)

        if not gaps:
            continue

        full_symbol = resolver.get_full_symbol(ticker)

        print(f"[{full_symbol}] {len(gaps)} gap(s) found:")
        ticker_filled = 0
        for gap_start, gap_end in gaps:
            print(f"  {gap_start} → {gap_end} ({(gap_end - gap_start).days + 1} days)")
            filled = fill_gap(ticker.id, full_symbol, gap_start, gap_end, exchange=ticker.exchange, currency=ticker.currency,
                              verified_shares=ticker.verified_shares)
            ticker_filled += filled
            print(f"  Filled {filled} rows.")

        if ticker_filled > 0:
            total_filled += ticker_filled
            tickers_filled += 1

    print(f"\nDone. Filled {total_filled} rows across {tickers_filled} tickers.")
