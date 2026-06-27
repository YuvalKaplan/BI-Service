import log
from datetime import date
from modules.core import api_stocks
from modules.object import batch_run, ticker, ticker_value, benchmark
from modules.object.ticker import Ticker
from modules.object.ticker_value import TickerValue

MARKET_CAP_THRESHOLD = 10_000_000_000  # $10B


def _fetch_all_screener_results() -> list[dict]:
    results: list[dict] = []
    for page in range(api_stocks.SCREENER_MAX_PAGES):
        page_data = api_stocks.fetch_company_screener(
            market_cap_more_than=MARKET_CAP_THRESHOLD,
            page=page,
            limit=api_stocks.SCREENER_PAGE_LIMIT,
        )
        results.extend(page_data)
        if len(page_data) < api_stocks.SCREENER_PAGE_LIMIT:
            break
    return results


def _resolve_tickers(screener_results: list[dict]) -> dict[str, tuple[int, str, float]]:
    """
    Resolve each screener result to a ticker_id.
    Returns {symbol: (ticker_id, country, market_cap)}.
    New tickers are upserted; existing ones get their market cap updated.
    """
    symbol_cache = ticker.fetch_all_for_symbol_cache()
    resolved: dict[str, tuple[int, str, float]] = {}
    today = date.today()

    for company in screener_results:
        symbol = company.get('symbol')
        market_cap = company.get('marketCap')
        country = company.get('country') or ''
        exchange = company.get('exchangeShortName') or ''
        name = company.get('companyName') or ''

        if not symbol or not market_cap or market_cap <= 0:
            continue

        ticker_id = symbol_cache.get(symbol)

        if ticker_id is None and symbol not in symbol_cache:
            # New ticker — upsert with screener data (no extra API call needed)
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
                symbol_cache[symbol] = ticker_id
            except Exception as e:
                log.record_notice(f"Failed to upsert ticker {symbol}: {e}")
                continue

        if ticker_id is None:
            continue  # symbol is known-invalid

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

        resolved[symbol] = (ticker_id, country, float(market_cap))

    return resolved


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


def run_blend_holdings() -> None:
    """
    Stage 1 (run BEFORE categorize_downloader):
    Fetch full large-cap universe from FMP screener, register all tickers,
    and populate the US Blend and Intl Blend benchmarks.
    """
    batch_run_id = batch_run.insert(batch_run.BatchRun(process='benchmark_generator_blend', activation='auto'))
    log.record_status(f"Starting Benchmark Generator (blend) batch job ID {batch_run_id}")
    try:
        screener_results = _fetch_all_screener_results()
        log.record_status(f"Fetched {len(screener_results)} large-cap companies from FMP screener.")

        resolved = _resolve_tickers(screener_results)
        log.record_status(f"Resolved {len(resolved)} tickers.")

        holding_date = date.today()
        us_items   = [(tid, mc) for _, (tid, country, mc) in resolved.items() if country == 'US']
        intl_items = [(tid, mc) for _, (tid, country, mc) in resolved.items() if country != 'US']
        log.record_status(f"Split: {len(us_items)} US, {len(intl_items)} International.")

        us_blend   = benchmark.fetch_by_region_and_style('US', 'blend')
        intl_blend = benchmark.fetch_by_region_and_style('International', 'blend')

        if us_blend:
            _build_and_store(us_blend.id, us_items, holding_date)
        if intl_blend:
            _build_and_store(intl_blend.id, intl_items, holding_date)

        batch_run.update_completed_at(batch_run_id)
        log.record_status("Benchmark Generator (blend) completed.\n")

    except Exception as e:
        log.record_error(f"Error in benchmark_generator blend: {e}")
        raise


