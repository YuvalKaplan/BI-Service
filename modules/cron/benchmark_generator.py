import log
import re
from datetime import date
from modules.const import LARGE_CAP_THRESHOLD
from modules.core import api_stocks
from modules.object import batch_run, ticker, ticker_value, benchmark
from modules.object.ticker import Ticker
from modules.object.ticker_value import TickerValue

US_EXCHANGES = ticker.US_EXCHANGES  # region classification is exchange-based, not ticker.country


# Stage 1: pull the eligible large-cap universe from the FMP screener (today only — no historical mode).
def _fetch_all_screener_results() -> list[dict]:
    results: list[dict] = []
    for page in range(api_stocks.SCREENER_MAX_PAGES):
        page_data = api_stocks.fetch_company_screener(
            market_cap_more_than=LARGE_CAP_THRESHOLD,
            page=page,
            limit=api_stocks.SCREENER_PAGE_LIMIT,
        )
        results.extend(page_data)
        if len(page_data) < api_stocks.SCREENER_PAGE_LIMIT:
            break
    return results


def _upsert_ticker(symbol: str, exchange: str, name: str, country: str) -> int | None:
    """Registers/updates the (symbol, exchange) row with screener data (no extra API call needed)."""
    t = Ticker(
        symbol=symbol,
        exchange=exchange,
        name=name,
        country=country,
        source='fmp',
        is_actively_trading=True,
    )
    try:
        ticker_id, _ = ticker.upsert_by_symbol(t)
        return ticker_id
    except Exception as e:
        log.record_notice(f"Failed to upsert ticker {symbol} ({exchange}): {e}")
        return None


# Stage 2: resolve each screener row to a ticker_id (registering new tickers as needed)
# and upsert today's market cap for it.
def _resolve_tickers(screener_results: list[dict]) -> dict[str, tuple[int, str, float]]:
    """
    Resolve each screener result to a ticker_id.
    Returns {symbol: (ticker_id, exchange, market_cap)} — one entry per symbol.
    New tickers are upserted; existing ones get their market cap updated.

    A symbol can have rows on multiple exchanges — either already in the DB, or across
    multiple rows within this one screener call (e.g. the same company cross-listed).
    Each row is resolved against its own exact (symbol, exchange) pair rather than
    blindly reusing whatever ticker_id the symbol maps to, so a row's market cap never
    gets attached to the wrong exchange's ticker. Among the resulting candidates for a
    symbol, the NYSE/NASDAQ-listed one is kept as the canonical resolved entry; if none
    of them is US-listed, the last one processed wins.
    """
    symbol_cache = ticker.fetch_all_for_symbol_cache()
    resolved: dict[str, tuple[int, str, float]] = {}
    today = date.today()

    for company in screener_results:
        raw_symbol = company.get('symbol')
        market_cap = company.get('marketCap')
        country = company.get('country') or ''
        exchange = company.get('exchangeShortName') or ''
        name = company.get('companyName') or ''

        if not raw_symbol or not market_cap or market_cap <= 0:
            continue

        # Strip exchange suffix (e.g. TD.TO → TD, SHOP.TO → SHOP) to match
        # how TickerResolver stores symbols in the ticker table.
        symbol = re.split(r'[\s.]', raw_symbol)[0]

        if symbol not in symbol_cache:
            # Brand new symbol — never seen before.
            ticker_id = _upsert_ticker(symbol, exchange, name, country)
            if ticker_id is None:
                continue
            symbol_cache[symbol] = (ticker_id, exchange)
        else:
            cached = symbol_cache[symbol]
            if cached is None:
                continue  # symbol is known-invalid
            cached_id, cached_exchange = cached
            if cached_exchange == exchange:
                ticker_id = cached_id
            else:
                # Same symbol, different exchange than the cached row — resolve this
                # exact (symbol, exchange) pair instead of reusing the cached ticker_id.
                ticker_id = _upsert_ticker(symbol, exchange, name, country)
                if ticker_id is None:
                    continue
                if ticker.is_preferred_exchange(exchange, cached_exchange):
                    symbol_cache[symbol] = (ticker_id, exchange)

        # Upsert today's market cap
        try:
            ticker_value.upsert(TickerValue(
                ticker_id=ticker_id,
                value_date=today,
                stock_price=company.get('price'),
                market_cap=float(market_cap),
            ))
        except Exception as e:
            log.record_notice(f"Failed to upsert market cap for {symbol}: {e}")

        existing = resolved.get(symbol)
        existing_exchange = existing[1] if existing else None
        if ticker.is_preferred_exchange(exchange, existing_exchange):
            resolved[symbol] = (ticker_id, exchange, float(market_cap))

    return resolved


# Stage 4: weight one region's items by market cap and store them as a benchmark_holding
# snapshot for holding_date. Shared as-is by both live ("today") and sim (historical dates).
def _build_and_store(
    benchmark_id: int,
    items: list[tuple[int, float]],  # (ticker_id, market_cap)
    holding_date: date,
) -> None:
    if not items:
        log.record_notice(f"No items for benchmark_id={benchmark_id} — skipping.")
        return
    total = sum(mc for _, mc in items)
    rows = [(ticker_id, mc, mc / total) for ticker_id, mc in items]
    benchmark.insert_holdings(benchmark_id, holding_date, rows)
    log.record_status(f"  benchmark_id={benchmark_id}: {len(rows)} holdings, total market cap ${total/1e12:.2f}T")


# Stage 3: split resolved tickers into the two regions each blend benchmark covers,
# by exchange (NYSE/NASDAQ = US) rather than ticker.country.
def _split_us_intl(
    rows: list[tuple[int, str, float]],  # (ticker_id, exchange, market_cap)
) -> tuple[list[tuple[int, float]], list[tuple[int, float]]]:
    """Splits resolved (ticker_id, exchange, market_cap) rows into (us_items, intl_items) pairs."""
    us_items   = [(tid, mc) for tid, exchange, mc in rows if exchange in US_EXCHANGES]
    intl_items = [(tid, mc) for tid, exchange, mc in rows if exchange not in US_EXCHANGES]
    return us_items, intl_items


# Stage 3b: look up the two target Benchmark rows (region/cap/style config) to store into.
def _fetch_blend_benchmarks() -> tuple[benchmark.Benchmark | None, benchmark.Benchmark | None]:
    us_blend   = benchmark.fetch_by_region_and_style('US', 'blend')
    intl_blend = benchmark.fetch_by_region_and_style('International', 'blend')
    return us_blend, intl_blend


def run() -> None:
    """
    Stage 1 (run BEFORE categorize_downloader):
    Fetch full large-cap universe from FMP screener, register all tickers,
    and populate the US Blend and Intl Blend benchmarks.
    """
    batch_run_id = batch_run.insert(batch_run.BatchRun(process='benchmark_generator', activation='auto'))
    log.record_status(f"Starting Benchmark Generator (blend) batch job ID {batch_run_id}")
    try:
        # Stage 1: fetch today's eligible large-cap universe.
        screener_results = _fetch_all_screener_results()
        log.record_status(f"Fetched {len(screener_results)} large-cap companies from FMP screener.")

        # Stage 2: resolve/register tickers and upsert today's market cap for each.
        resolved = _resolve_tickers(screener_results)
        log.record_status(f"Resolved {len(resolved)} tickers.")

        # Stage 3: split into US / International and resolve the two blend Benchmark rows.
        holding_date = date.today()
        us_items, intl_items = _split_us_intl(list(resolved.values()))
        log.record_status(f"Split: {len(us_items)} US, {len(intl_items)} International.")

        us_blend, intl_blend = _fetch_blend_benchmarks()

        # Stage 4: weight and store one snapshot per region for today's holding_date.
        if us_blend:
            _build_and_store(us_blend.id, us_items, holding_date)
        if intl_blend:
            _build_and_store(intl_blend.id, intl_items, holding_date)

        batch_run.update_completed_at(batch_run_id)
        log.record_status("Benchmark Generator (blend) completed.\n")

    except Exception as e:
        log.record_error(f"Error in benchmark_generator blend: {e}")
        raise


