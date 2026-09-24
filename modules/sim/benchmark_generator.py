import log
from datetime import date, timedelta
from modules.core import api_stocks
from modules.object import batch_run, ticker
from modules.ticker.resolver import TickerResolver
from modules.ticker import pricing
from modules.ticker import util as tu
from modules.cron.benchmark_generator import (
    _fetch_all_screener_results,
    _resolve_tickers,
    _split_us_intl,
    _fetch_blend_benchmarks,
    _build_and_store,
)

WINDOW_DAYS = 5    # how far from a Wednesday to search for the closest available market cap
DATA_LAG_DAYS = 3  # FMP's historical price/market-cap endpoints don't have data yet for the most recent days


def _most_recent_weekday(d: date) -> date:
    """Steps back from d to the nearest Mon-Fri date — a fixed calendar-day lag can land on a
    Saturday/Sunday (e.g. 3 days back from a Wednesday is a Sunday), and there's no trading
    data at all for a non-weekday, which would otherwise fail validation for every ticker."""
    while d.weekday() >= 5:  # Saturday=5, Sunday=6
        d -= timedelta(days=1)
    return d


def _wednesdays_between(start: date, end: date) -> list[date]:
    offset = (2 - start.weekday()) % 7
    first = start + timedelta(days=offset)
    wednesdays = []
    d = first
    while d <= end:
        wednesdays.append(d)
        d += timedelta(days=7)
    return wednesdays


def _consolidate_for_date(
    rows: list[tuple[int, str, float]],  # (ticker_id, exchange, market_cap) for ONE historical date
    master_lookup: dict[int, int | None],
    exchange_lookup: dict[int, str],  # authoritative ticker.exchange, keyed by ticker_id
) -> list[tuple[int, str, float]]:
    """
    Collapses share-class siblings onto their master ticker for a single historical
    snapshot date. Unlike the live path, the combined cap can't be read from the persisted
    accumulated_market_cap column (that only reflects today) — it's reconstructed by summing
    whichever siblings have a historical market cap for this exact date.

    The output exchange (used for US/International bucketing) always comes from the master's
    own registered exchange via exchange_lookup, never from whichever sibling happened to have
    price data on this particular date — otherwise a company could flip between the US and
    International bucket week to week purely depending on which listing's history was
    available that day (e.g. a master whose own row is missing for this date would previously
    fall back to an arbitrary sibling's exchange instead).
    """
    by_master: dict[int, float] = {}
    for tid, _exchange, mc in rows:
        eff_id = master_lookup.get(tid) or tid
        by_master[eff_id] = by_master.get(eff_id, 0.0) + mc

    return [(eff_id, exchange_lookup.get(eff_id, ''), total_mc) for eff_id, total_mc in by_master.items()]


def run(inception_date: date) -> list[tuple[str, date, int]]:
    """
    Backfills historical Wednesday-dated benchmark_holding rows for the US and
    International blend benchmarks, from inception_date through today minus
    DATA_LAG_DAYS (FMP's historical endpoints don't have price/market-cap data yet for the
    most recent few days) — otherwise the same weekday the live cron refreshes the benchmark on.

    Returns a (benchmark_name, holding_date, num_holdings) row per snapshot actually stored,
    for callers that want to report on what was created (e.g. scripts/sim_prep_data.py).

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
        data_cutoff_date = _most_recent_weekday(today - timedelta(days=DATA_LAG_DAYS))

        # Stage 1: fetch today's eligible large-cap universe (same as live — no historical mode exists).
        screener_results = _fetch_all_screener_results()
        log.record_status(f"Fetched {len(screener_results)} large-cap companies from FMP screener.")

        # Stage 2: resolve/register tickers, validating/storing each one's market cap as of
        # data_cutoff_date rather than literal today — FMP hasn't published today's close yet
        # for most tickers. require_validated_price=False so a withheld/failed validation on
        # this one date doesn't drop the ticker from Stage 3's historical fetch entirely —
        # Stage 3 re-fetches its own full historical series regardless, so a bad/withheld
        # data point on the current date shouldn't throw away an otherwise-fine ticker's
        # whole backfill (this was previously excluding real companies like Adobe from every
        # historical Wednesday whenever today's specific value happened to be withheld).
        resolved = _resolve_tickers(screener_results, value_date=data_cutoff_date, require_validated_price=False)
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

            mc_raw = api_stocks.get_stock_historic_market_cap(full_symbol, inception_date, data_cutoff_date)
            if isinstance(mc_raw, str):
                log.record_notice(f"Historic market cap unavailable for {full_symbol}: {mc_raw}")
                continue

            # FMP reports market cap in the security's native currency — convert to USD before
            # it ever enters `history`, same as the live/pricing.py path, so every consumer
            # downstream (benchmark weighting, accumulated_market_cap, thresholds) can trust
            # these values are uniformly USD. Uses that date's own historical FX rate, not a
            # single blanket rate — rates move meaningfully over a multi-month backfill range.
            currency = tu.currency_for_exchange(exchange)
            fx_rates = pricing.fetch_historic_usd_rates(currency, inception_date, data_cutoff_date)
            if isinstance(fx_rates, str):
                log.record_notice(f"FX rates unavailable for {full_symbol}'s currency ({currency}) — skipping.")
                continue

            series: dict[date, float] = {}
            for row in mc_raw:
                try:
                    d = date.fromisoformat(row["date"])
                    raw_mc = float(row["marketCap"])
                except (KeyError, ValueError, TypeError):
                    continue
                if not fx_rates:  # currency is None/USD - no conversion needed
                    series[d] = raw_mc
                    continue
                rate = pricing.closest_value_for_date(fx_rates, d, window_days=WINDOW_DAYS)
                if rate is None:
                    continue  # no FX rate close enough to this date to trust - drop it
                series[d] = raw_mc * rate
            if series:
                history[ticker_id] = series

        log.record_status(f"Fetched historical market cap series for {len(history)} tickers.")

        # Stage 4: resolve the two blend Benchmark rows once, reused across every Wednesday below.
        us_blend, intl_blend = _fetch_blend_benchmarks()

        # Stage 4b: master-ticker assignment is a permanent identity fact, not time-varying,
        # so it's fetched once and reused for every historical date in the loop below.
        master_lookup = {
            tid: master_id for tid, (master_id, _acc_cap) in ticker.fetch_master_info_by_ids(ticker_ids).items()
        }

        # Stage 4c: authoritative exchange per master, for _consolidate_for_date. A master
        # not itself among ticker_ids (only a sibling cleared the threshold this week) still
        # needs its own registered exchange looked up directly, same as the live path.
        exchange_lookup = {tid: t.exchange or '' for tid, t in tickers_by_id.items()}
        missing_master_ids = set(master_lookup.values()) - {None} - set(exchange_lookup.keys())
        if missing_master_ids:
            exchange_lookup.update({t.id: t.exchange or '' for t in ticker.fetch_by_ids(list(missing_master_ids))})

        # Stage 5 (sim-only): for each historical Wednesday, look up each ticker's closest
        # available market cap, consolidate share-class siblings, and build/store one snapshot
        # per region — same math as live's single "today" snapshot (_split_us_intl +
        # _build_and_store), just repeated per date.
        wednesdays = _wednesdays_between(inception_date, data_cutoff_date)
        log.record_status(f"Backfilling {len(wednesdays)} historical Wednesdays from {inception_date} to {data_cutoff_date}.")

        benchmarks_created: list[tuple[str, date, int]] = []

        for wednesday in wednesdays:
            rows: list[tuple[int, str, float]] = []
            for ticker_id, series in history.items():
                mc = pricing.closest_value_for_date(series, wednesday, window_days=WINDOW_DAYS)
                if mc is None or mc <= 0:
                    continue
                rows.append((ticker_id, exchange_by_ticker.get(ticker_id, ''), mc))

            rows = _consolidate_for_date(rows, master_lookup, exchange_lookup)
            us_items, intl_items = _split_us_intl(rows)

            if us_blend and us_items:
                _build_and_store(us_blend.id, us_blend.name, us_items, wednesday)
                benchmarks_created.append((us_blend.name, wednesday, len(us_items)))
            if intl_blend and intl_items:
                _build_and_store(intl_blend.id, intl_blend.name, intl_items, wednesday)
                benchmarks_created.append((intl_blend.name, wednesday, len(intl_items)))

        batch_run.update_completed_at(batch_run_id)
        log.record_status("Sim Benchmark Generator completed.\n")
        return benchmarks_created

    except Exception as e:
        log.record_error(f"Error in sim benchmark_generator: {e}")
        raise
