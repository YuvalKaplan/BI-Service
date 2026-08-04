import log
from datetime import date, timedelta
from modules.core import api_stocks
from modules.object import batch_run, ticker
from modules.ticker.resolver import TickerResolver
from modules.cron.benchmark_generator import (
    _fetch_all_screener_results,
    _resolve_tickers,
    _split_us_intl,
    _fetch_blend_benchmarks,
    _build_and_store,
)

WINDOW_DAYS = 5  # how far from a Sunday to search for the closest available market cap


def _sundays_between(start: date, end: date) -> list[date]:
    offset = (6 - start.weekday()) % 7
    first = start + timedelta(days=offset)
    sundays = []
    d = first
    while d <= end:
        sundays.append(d)
        d += timedelta(days=7)
    return sundays


def _closest_market_cap(series: dict[date, float], target: date) -> float | None:
    for offset in range(0, WINDOW_DAYS + 1):
        for d in (target - timedelta(days=offset), target + timedelta(days=offset)):
            if d in series:
                return series[d]
    return None


def run(inception_date: date) -> None:
    """
    Backfills historical Sunday-dated benchmark_holding rows for the US and
    International blend benchmarks, from inception_date through today —
    the same weekday the live cron refreshes the benchmark on.

    The eligible large-cap universe is fixed from today's FMP screener call
    (the screener has no historical mode); each resolved ticker's historical
    market cap is then pulled via the FMP historical-market-capitalization
    endpoint, which does support a date range. Snapshot building (exchange
    split + weighting + storage) reuses the same helpers
    modules.cron.benchmark_generator.run() uses for "today".
    """
    batch_run_id = batch_run.insert(batch_run.BatchRun(process='sim_benchmark_gen', activation='auto'))
    log.record_status(f"Starting Sim Benchmark Generator batch job ID {batch_run_id}")
    try:
        today = date.today()

        # Stage 1: fetch today's eligible large-cap universe (same as live — no historical mode exists).
        screener_results = _fetch_all_screener_results()
        log.record_status(f"Fetched {len(screener_results)} large-cap companies from FMP screener.")

        # Stage 2: resolve/register tickers (also upserts today's market cap as a side effect, same as live).
        resolved = _resolve_tickers(screener_results)
        log.record_status(f"Resolved {len(resolved)} tickers.")

        # Stage 3 (sim-only): for each resolved ticker, pull its full historical market-cap
        # series in one call — this is what makes historical (not just "today") snapshots possible.
        ticker_ids = [tid for tid, _exchange, _mc in resolved.values()]
        tickers_by_id = {t.id: t for t in ticker.fetch_by_ids(ticker_ids)}
        resolver = TickerResolver(TickerResolver.POPULATE_TICKER)

        history: dict[int, dict[date, float]] = {}
        exchange_by_ticker: dict[int, str] = {}

        for _symbol, (ticker_id, exchange, _today_mc) in resolved.items():
            t = tickers_by_id.get(ticker_id)
            if t is None:
                continue
            exchange_by_ticker[ticker_id] = exchange
            # Reconstruct FMP's original symbol (e.g. "TD.TO") from our stripped
            # ticker.symbol + ticker.exchange — required by the historical endpoint below.
            full_symbol = resolver.get_full_symbol(t)

            mc_raw = api_stocks.get_stock_historic_market_cap(full_symbol, inception_date, today)
            if isinstance(mc_raw, str):
                log.record_notice(f"Historic market cap unavailable for {full_symbol}: {mc_raw}")
                continue

            series: dict[date, float] = {}
            for row in mc_raw:
                try:
                    series[date.fromisoformat(row["date"])] = float(row["marketCap"])
                except (KeyError, ValueError, TypeError):
                    continue
            if series:
                history[ticker_id] = series

        log.record_status(f"Fetched historical market cap series for {len(history)} tickers.")

        # Stage 4: resolve the two blend Benchmark rows once, reused across every Sunday below.
        us_blend, intl_blend = _fetch_blend_benchmarks()

        # Stage 5 (sim-only): for each historical Sunday, look up each ticker's closest
        # available market cap and build/store one snapshot per region — same math as live's
        # single "today" snapshot (_split_us_intl + _build_and_store), just repeated per date.
        sundays = _sundays_between(inception_date, today)
        log.record_status(f"Backfilling {len(sundays)} historical Sundays from {inception_date} to {today}.")

        for sunday in sundays:
            rows: list[tuple[int, str, float]] = []
            for ticker_id, series in history.items():
                mc = _closest_market_cap(series, sunday)
                if mc is None or mc <= 0:
                    continue
                rows.append((ticker_id, exchange_by_ticker.get(ticker_id, ''), mc))

            us_items, intl_items = _split_us_intl(rows)

            if us_blend:
                _build_and_store(us_blend.id, us_items, sunday)
            if intl_blend:
                _build_and_store(intl_blend.id, intl_items, sunday)

        batch_run.update_completed_at(batch_run_id)
        log.record_status("Sim Benchmark Generator completed.\n")

    except Exception as e:
        log.record_error(f"Error in sim benchmark_generator: {e}")
        raise
