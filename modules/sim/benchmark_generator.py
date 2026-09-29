import log
from datetime import date, timedelta
from modules.object import batch_run, benchmark, ticker, ticker_value, screener_listing, universe_company
from modules.object.screener_listing import ScreenerListing
from modules.ticker import company, pricing
from modules.ticker import util as tu
from modules.cron.benchmark_generator import current_companies, company_styles, select_holdings, store_holdings

WINDOW_DAYS = 5    # how far from a Wednesday to search for the closest available market cap
DATA_LAG_DAYS = 3  # FMP's historical price/market-cap endpoints don't have data yet for the most recent days


def _most_recent_weekday(d: date) -> date:
    """Steps back from d to the nearest Mon-Fri date — a fixed calendar-day lag can land on a
    Saturday/Sunday (e.g. 3 days back from a Wednesday is a Sunday), and there's no trading
    data at all for a non-weekday, which would otherwise fail validation for every ticker."""
    while d.weekday() >= 5:  # Saturday=5, Sunday=6
        d -= timedelta(days=1)
    return d


def data_cutoff_date() -> date:
    """The latest date FMP's historical endpoints reliably have data for: the sim's screen date
    (scripts/sim_prep_data.py) and the end of its benchmark backfill."""
    return _most_recent_weekday(date.today() - timedelta(days=DATA_LAG_DAYS))


def _wednesdays_between(start: date, end: date) -> list[date]:
    offset = (2 - start.weekday()) % 7
    first = start + timedelta(days=offset)
    wednesdays = []
    d = first
    while d <= end:
        wednesdays.append(d)
        d += timedelta(days=7)
    return wednesdays


def _company_caps_for_date(
    wednesday: date,
    history: dict[int, dict[date, float]],
    listing_order: dict[int, list[int]],   # company id -> its listings with history, primary first
    region_lookup: dict[int, str],         # company id -> 'US' | 'International'
) -> list[tuple[int, str, float]]:
    """
    One (company_id, region, market_cap) row per company for a single historical date. FMP
    reports the whole company's cap on every listing, so listings are never summed: the cap is
    the primary listing's value closest to that date, falling back to the next listing (in
    primary order) with data. Each benchmark's market_cap_min is applied per date by
    select_holdings.
    """
    rows: list[tuple[int, str, float]] = []
    for cid, listing_ids in listing_order.items():
        for tid in listing_ids:
            mc = pricing.closest_value_for_date(history[tid], wednesday, window_days=WINDOW_DAYS)
            if mc and mc > 0:
                rows.append((cid, region_lookup[cid], mc))
                break
    return rows


def run(inception_date: date) -> tuple[date, list[tuple[str, date, int]]]:
    """
    Backfills historical Wednesday-dated benchmark_holding rows for every enabled benchmark,
    from inception_date through the data cutoff (FMP's historical endpoints don't have
    price/market-cap data yet for the most recent few days) — otherwise the same weekday the
    live cron refreshes the benchmarks on.

    The universe is the one scripts/sim_prep_data.py stored (it screens, syncs masters and builds
    it, dated at the data cutoff — the screener has no historical mode, so today's large-cap
    universe is used); failing that, the latest stored one.
    Each company's screened listings then get their historical market-cap series — the stored
    values when they cover every Wednesday, else FMP's history — and for each Wednesday every
    company takes its primary listing's value; the benchmarks are formed with the same selection
    and weighting as live (select_holdings, store_holdings).

    Returns the universe's screen date and a (benchmark_name, holding_date, num_holdings) row per
    snapshot stored, for callers that report on it (scripts/sim_benchmark.py).
    """
    batch_run_id = batch_run.insert(batch_run.BatchRun(process='sim_benchmark_gen', activation='auto'))
    log.record_status(f"Starting Sim Benchmark Generator batch job ID {batch_run_id}")
    try:
        # The universe sim_prep_data.py built (dated at the data cutoff), else the latest one.
        cutoff = data_cutoff_date()
        screen_date = universe_company.fetch_latest_date(up_to=cutoff) or universe_company.fetch_latest_date()
        if screen_date is None:
            raise Exception("No stored universe — run scripts/sim_prep_data.py first.")

        # The universe's companies (through their current masters) and the screened listings of
        # each: registered home-market lines and admitted foreign lines.
        companies = current_companies(universe_company.fetch_for_date(screen_date))
        region_lookup = {cid: region for cid, region, _mc in companies}
        listings = [l for l in screener_listing.fetch_for_date(screen_date) if l.ticker_id]
        masters = ticker.fetch_master_info_by_ids([l.ticker_id for l in listings])
        members: dict[int, list[ScreenerListing]] = {}
        for l in listings:
            cid = (masters.get(l.ticker_id) or (None, None))[0] or l.ticker_id
            if cid in region_lookup:
                members.setdefault(cid, []).append(l)
        tickers_by_id = {t.id: t for t in ticker.fetch_by_ids(
            list({l.ticker_id for ms in members.values() for l in ms} | set(region_lookup)))}

        wednesdays = _wednesdays_between(inception_date, cutoff)
        if not wednesdays:
            raise Exception(f"No Wednesday between {inception_date} and the data cutoff {cutoff}.")

        # Each listing's market-cap series. The stored values come first: they're the ones live
        # validated (USD at each date's FX rate, from the listing's own currency; FMP glitches
        # repaired — scripts/data_fill_ticker_value_refresh.py --outliers / --profile-check for
        # older history), and most listings — ETF holdings valued daily, weekly-screened lines —
        # have one near every Wednesday. Only a listing whose stored values miss a Wednesday
        # (registered recently, or not in use then) gets its full history from FMP, in one call —
        # the live path's fetch (pricing.fetch_price_and_market_cap_history): USD at each date's
        # own historical FX rate, and FMP glitches repaired from the price or dropped
        # (pricing.clean_market_caps: Compass at 1/100 for two weeks would otherwise fall out of
        # the $10B benchmark on those dates), with history from before inception as context and
        # the screener quote's share count as the reference.
        listing_ids = [l.ticker_id for ms in members.values() for l in ms if l.ticker_id in tickers_by_id]
        stored = ticker_value.fetch_market_caps_between(
            listing_ids, wednesdays[0] - timedelta(days=WINDOW_DAYS), wednesdays[-1] + timedelta(days=WINDOW_DAYS))
        history: dict[int, dict[date, float]] = {}
        from_store = fetched_count = 0
        for ms in members.values():
            for l in ms:
                t = tickers_by_id.get(l.ticker_id)
                if t is None:
                    continue
                series = stored.get(l.ticker_id)
                if series and all(pricing.closest_value_for_date(series, w, window_days=WINDOW_DAYS) for w in wednesdays):
                    history[l.ticker_id] = series
                    from_store += 1
                    continue
                fetched = pricing.fetch_price_and_market_cap_history(
                    l.symbol,
                    inception_date - timedelta(days=pricing.OUTLIER_CONTEXT_DAYS),
                    cutoff,
                    currency=tu.listing_currency(l.exchange, t.currency),
                    reference_shares=l.quote_shares,
                    verified_shares=t.verified_shares,
                )
                if isinstance(fetched, str):
                    log.record_notice(f"Historic market cap unavailable for {l.symbol}: {fetched}")
                    continue
                series = {d: market_cap for d, (_price, market_cap) in fetched.items()}
                if series:
                    history[l.ticker_id] = series
                    fetched_count += 1
        log.record_status(
            f"Market cap series for {len(history)} listings of {len(members)} companies: "
            f"{from_store} from stored values, {fetched_count} fetched from FMP.")

        # Each company's listings with history, primary first, for per-date cap lookup.
        listing_order: dict[int, list[int]] = {}
        for cid, ms in members.items():
            candidates = [tickers_by_id[l.ticker_id] for l in ms if l.ticker_id in tickers_by_id]
            if cid in tickers_by_id and all(t.id != cid for t in candidates):
                candidates.append(tickers_by_id[cid])
            order = [t.id for t in company.ordered_listings(candidates, cid) if t.id in history]
            if order:
                listing_order[cid] = order

        benchmarks = benchmark.fetch_all()
        styles = company_styles(list(listing_order), benchmarks)
        log.record_status(f"Backfilling {len(wednesdays)} historical Wednesdays from {inception_date} to {cutoff}.")

        benchmarks_created: list[tuple[str, date, int]] = []
        for wednesday in wednesdays:
            rows = _company_caps_for_date(wednesday, history, listing_order, region_lookup)
            selected = select_holdings(rows, benchmarks, styles)
            for b in benchmarks:
                if selected[b.id]:
                    store_holdings(b, selected[b.id], wednesday)
                    benchmarks_created.append((b.name, wednesday, len(selected[b.id])))

        batch_run.update_completed_at(batch_run_id)
        log.record_status("Sim Benchmark Generator completed.\n")
        return screen_date, benchmarks_created

    except Exception as e:
        log.record_error(f"Error in sim benchmark_generator: {e}")
        raise
