import log
from datetime import date, timedelta
from modules.const import LARGE_CAP_THRESHOLD
from modules.core import api_stocks
from modules.object import batch_run, ticker
from modules.object.ticker import Ticker
from modules.ticker.resolver import TickerResolver
from modules.ticker import company, pricing
from modules.ticker import master as ticker_master
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
    wednesday: date,
    history: dict[int, dict[date, float]],
    listing_order: dict[int, list[int]],   # company id -> its listings with history, primary first
    region_lookup: dict[int, str],         # company id -> 'US' | 'International'
) -> list[tuple[int, str, float]]:
    """
    One (company_id, region, market_cap) row per company for a single historical date. FMP
    reports the whole company's cap on every listing, so listings are never summed: the cap is
    the primary listing's value closest to that date, falling back to the next listing (in
    primary order) with data. Companies below the $10B USD floor on that date are dropped.
    """
    rows: list[tuple[int, str, float]] = []
    for cid, listing_ids in listing_order.items():
        for tid in listing_ids:
            mc = pricing.closest_value_for_date(history[tid], wednesday, window_days=WINDOW_DAYS)
            if mc and mc > 0:
                if mc >= LARGE_CAP_THRESHOLD:
                    rows.append((cid, region_lookup.get(cid, company.INTERNATIONAL), mc))
                break
    return rows


def run(inception_date: date) -> list[tuple[str, date, int]]:
    """
    Backfills historical Wednesday-dated benchmark_holding rows for the US and
    International blend benchmarks, from inception_date through today minus
    DATA_LAG_DAYS (FMP's historical endpoints don't have price/market-cap data yet for the
    most recent few days) — otherwise the same weekday the live cron refreshes the benchmark on.

    Returns a (benchmark_name, holding_date, num_holdings) row per snapshot actually stored,
    for callers that want to report on what was created (e.g. scripts/sim_benchmark.py).

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

        # Stage 2a: link any newly seen listings to their company and refresh company regions.
        ticker_master.sync_masters_and_company_data()

        # Stage 3 (sim-only): for each resolved ticker, pull its full historical market-cap
        # series in one call — this is what makes historical (not just "today") snapshots possible.
        ticker_ids = [tid for tid, _exchange, _mc in resolved.values()]
        tickers_by_id = {t.id: t for t in ticker.fetch_by_ids(ticker_ids)}
        resolver = TickerResolver(TickerResolver.POPULATE_TICKER)

        history: dict[int, dict[date, float]] = {}

        for _symbol, (ticker_id, exchange, _today_mc) in resolved.items():
            t = tickers_by_id.get(ticker_id)
            if t is None:
                continue
            # Reconstruct FMP's original symbol (e.g. "TD.TO") from our stripped
            # ticker.symbol + ticker.exchange — required by the historical endpoint below.
            full_symbol = resolver.get_full_symbol(t)

            mc_raw = api_stocks.get_stock_historic_market_cap(full_symbol, inception_date, data_cutoff_date)
            if isinstance(mc_raw, str):
                log.record_notice(f"Historic market cap unavailable for {full_symbol}: {mc_raw}")
                continue

            # FMP reports market cap in the security's native currency — convert to USD before
            # it ever enters `history`, same as the live/pricing.py path, so every consumer
            # downstream (benchmark weighting, company caps, the $10B floor) can trust
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

        # Stage 4b: group listings by company (master assignment is a permanent identity fact,
        # so fetched once) and order each company's listings primary-first, for per-date cap
        # lookup; region comes from the company (refreshed by the sync in Stage 2a).
        master_lookup = {
            tid: master_id for tid, (master_id, _cap) in ticker.fetch_master_info_by_ids(list(history)).items()
        }
        company_of = {tid: master_lookup.get(tid) or tid for tid in history}
        missing = set(company_of.values()) - set(tickers_by_id)
        if missing:
            tickers_by_id.update({t.id: t for t in ticker.fetch_by_ids(list(missing))})
        members: dict[int, list[Ticker]] = {}
        for tid, cid in company_of.items():
            if tid in tickers_by_id:
                members.setdefault(cid, []).append(tickers_by_id[tid])
        listing_order: dict[int, list[int]] = {}
        region_lookup: dict[int, str] = {}
        for cid, ms in members.items():
            candidates = ms + ([tickers_by_id[cid]] if cid in tickers_by_id and all(m.id != cid for m in ms) else [])
            listing_order[cid] = [t.id for t in company.ordered_listings(candidates, cid) if t.id in history]
            c = tickers_by_id.get(cid)
            region_lookup[cid] = (c.region if c and c.region else company.region(candidates, cid))

        # Stage 5 (sim-only): for each historical Wednesday, look up each ticker's closest
        # available market cap, consolidate share-class siblings, and build/store one snapshot
        # per region — same math as live's single "today" snapshot (_split_us_intl +
        # _build_and_store), just repeated per date.
        wednesdays = _wednesdays_between(inception_date, data_cutoff_date)
        log.record_status(f"Backfilling {len(wednesdays)} historical Wednesdays from {inception_date} to {data_cutoff_date}.")

        benchmarks_created: list[tuple[str, date, int]] = []

        for wednesday in wednesdays:
            rows = _consolidate_for_date(wednesday, history, listing_order, region_lookup)
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
