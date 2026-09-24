import log
import re
from datetime import date
from modules.const import LARGE_CAP_THRESHOLD
from modules.core import api_stocks
from modules.object import batch_run, ticker, benchmark
from modules.object.ticker import Ticker
from modules.ticker import pricing

US_EXCHANGES = ticker.US_EXCHANGES  # region classification is exchange-based, not ticker.country

# FMP's unfiltered company-screener call is heavily US/Canada-biased (confirmed empirically:
# an unfiltered call surfaces only a handful of XETRA-listed companies, while an explicit
# exchange=XETRA call returns the full ~300-company German large-cap universe) — the
# unfiltered call already covers NYSE/NASDAQ/TSX/AMEX well, so these are the additional
# exchanges queried explicitly to get meaningful international coverage.
INTERNATIONAL_EXCHANGES = [
    'JPX', 'XETRA', 'HKSE', 'ASX', 'SIX', 'PAR', 'SHH', 'NSE', 'TAI', 'BSE',
    'SHZ', 'STO', 'KSC', 'MIL', 'FSX', 'SAO', 'AMS', 'WSE', 'SES', 'BME',
    'OSL', 'VIE', 'JNB',
]


# Stage 1: pull the eligible large-cap universe from the FMP screener (today only — no historical mode).
def _fetch_all_screener_results() -> list[dict]:
    """
    One unfiltered call (covers NYSE/NASDAQ/TSX/AMEX well) plus one exchange-filtered call per
    entry in INTERNATIONAL_EXCHANGES, each paginated the same way. Results are de-duped by
    (symbol, exchange) — the two call sets shouldn't normally overlap, but a company that's
    somehow surfaced by more than one call should still only be processed once downstream.
    """
    seen: set[tuple[str | None, str | None]] = set()
    results: list[dict] = []

    def _collect(exchange: str | None) -> None:
        for page in range(api_stocks.SCREENER_MAX_PAGES):
            page_data = api_stocks.fetch_company_screener(
                market_cap_more_than=LARGE_CAP_THRESHOLD,
                page=page,
                limit=api_stocks.SCREENER_PAGE_LIMIT,
                exchange=exchange,
            )
            for row in page_data:
                key = (row.get('symbol'), row.get('exchangeShortName'))
                if key in seen:
                    continue
                seen.add(key)
                results.append(row)
            if len(page_data) < api_stocks.SCREENER_PAGE_LIMIT:
                break

    _collect(None)
    for exchange in INTERNATIONAL_EXCHANGES:
        _collect(exchange)

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
# and upsert its market cap for value_date.
def _resolve_tickers(
    screener_results: list[dict],
    value_date: date | None = None,
    require_validated_price: bool = True,
) -> dict[str, tuple[int, str, float]]:
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

    value_date defaults to today (live's "today" snapshot). The sim path passes its own
    data_cutoff_date instead — validating/storing against literal today would fail for most
    tickers (FMP hasn't published today's close yet).

    require_validated_price controls what happens when that validation is withheld/fails
    (e.g. a mismatch vs stored history still inside its grace period): live (default True)
    excludes the ticker from the returned dict entirely, since a live "today" snapshot needs
    a genuinely validated price. The sim path passes False — sim only uses this dict to
    discover the ticker universe for Stage 3's own historical-series fetch (the market_cap
    value returned here is never used downstream in sim), so one bad/withheld data point on
    the *current* date shouldn't throw away an otherwise-fine ticker's entire historical
    backfill. In that case the screener's own reported market_cap is used as a placeholder.
    """
    symbol_cache = ticker.fetch_all_for_symbol_cache()
    resolved: dict[str, tuple[int, str, float]] = {}
    if value_date is None:
        value_date = date.today()

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

        # Validate this date's market cap against FMP history before trusting it.
        validated = pricing.store_validated_ticker_value(ticker_id, raw_symbol, value_date, exchange=exchange)
        if validated is not None:
            resolved_market_cap = validated.market_cap
        elif require_validated_price:
            continue  # withheld (grace period) or flagged invalid - exclude from this week's benchmark
        else:
            # sim: keep the ticker in the universe despite the withheld/failed validation —
            # this value is only a placeholder, never used downstream (see docstring above).
            resolved_market_cap = market_cap

        existing = resolved.get(symbol)
        existing_exchange = existing[1] if existing else None
        if ticker.is_preferred_exchange(exchange, existing_exchange):
            resolved[symbol] = (ticker_id, exchange, resolved_market_cap)

    return resolved


# Stage 2b: collapse share-class siblings onto their master ticker before weighting, so a
# multi-ticker company (e.g. GOOGL/GOOG) contributes one combined row instead of two split ones.
def _apply_master_consolidation(
    resolved: dict[str, tuple[int, str, float]],
) -> list[tuple[int, str, float]]:
    """
    - A sibling (ticker.master_ticker_id set) contributes nothing of its own; its company
      is represented once via the master row, using accumulated_market_cap.
    - The master is force-included via its DB-stored exchange even if it didn't itself
      clear the screener's threshold this week (only a sibling did) — accumulated_market_cap
      is precisely what makes that possible without a second screener/profile call.
    - Standalone tickers (no master) pass through unchanged.
    """
    ids = [tid for tid, _ex, _mc in resolved.values()]
    tickers_by_id = {t.id: t for t in ticker.fetch_by_ids(ids)}
    missing_masters = {
        t.master_ticker_id for t in tickers_by_id.values() if t.master_ticker_id
    } - set(tickers_by_id.keys())
    if missing_masters:
        tickers_by_id.update({t.id: t for t in ticker.fetch_by_ids(list(missing_masters))})

    out: dict[int, tuple[int, str, float]] = {}
    for tid, exchange, market_cap in resolved.values():
        t = tickers_by_id.get(tid)
        if t and t.master_ticker_id:
            master = tickers_by_id.get(t.master_ticker_id)
            if master and master.exchange:
                out[master.id] = (master.id, master.exchange, master.accumulated_market_cap or market_cap)
                continue
        eff_cap = t.accumulated_market_cap if (t and t.accumulated_market_cap) else market_cap
        out[tid] = (tid, exchange, eff_cap)
    return list(out.values())


# Stage 4: weight one region's items by market cap and store them as a benchmark_holding
# snapshot for holding_date. Shared as-is by both live ("today") and sim (historical dates).
def _build_and_store(
    benchmark_id: int,
    benchmark_name: str,
    items: list[tuple[int, float]],  # (ticker_id, market_cap)
    holding_date: date,
) -> None:
    if not items:
        log.record_notice(f"No items for {benchmark_name} (id={benchmark_id}) on {holding_date} — skipping.")
        return
    total = sum(mc for _, mc in items)
    rows = [(ticker_id, mc, mc / total) for ticker_id, mc in items]
    benchmark.insert_holdings(benchmark_id, holding_date, rows)
    log.record_status(
        f"  {benchmark_name} (id={benchmark_id}) on {holding_date}: "
        f"{len(rows)} holdings, total market cap ${total/1e12:.2f}T"
    )


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

        # Stage 2b: collapse share-class siblings onto their master ticker.
        consolidated = _apply_master_consolidation(resolved)
        log.record_status(f"Consolidated to {len(consolidated)} companies.")

        # Stage 3: split into US / International and resolve the two blend Benchmark rows.
        holding_date = date.today()
        us_items, intl_items = _split_us_intl(consolidated)
        log.record_status(f"Split: {len(us_items)} US, {len(intl_items)} International.")

        us_blend, intl_blend = _fetch_blend_benchmarks()

        # Stage 4: weight and store one snapshot per region for today's holding_date.
        if us_blend:
            _build_and_store(us_blend.id, us_blend.name, us_items, holding_date)
        if intl_blend:
            _build_and_store(intl_blend.id, intl_blend.name, intl_items, holding_date)

        batch_run.update_completed_at(batch_run_id)
        log.record_status("Benchmark Generator (blend) completed.\n")

    except Exception as e:
        log.record_error(f"Error in benchmark_generator blend: {e}")
        raise


