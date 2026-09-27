import log
import re
from dataclasses import dataclass
from datetime import date, timedelta
from modules.const import LARGE_CAP_THRESHOLD
from modules.core import api_stocks
from modules.object import batch_run, ticker, benchmark
from modules.object.ticker import Ticker
from modules.ticker import company, pricing
from modules.ticker import master as ticker_master
from modules.ticker import util as tu

# FMP's screener has to be queried per exchange to get real international coverage (an
# unfiltered call is heavily US/Canada-biased and mixes currencies), so the universe is the
# union of these exchanges' large caps.
US_SCREENER_EXCHANGES = ['NYSE', 'NASDAQ', 'AMEX']
INTERNATIONAL_EXCHANGES = [
    'TSX',
    'JPX', 'XETRA', 'HKSE', 'ASX', 'SIX', 'PAR', 'SHH', 'NSE', 'TAI', 'BSE',
    'SHZ', 'STO', 'KSC', 'MIL', 'FSX', 'SAO', 'AMS', 'WSE', 'SES', 'BME',
    'OSL', 'VIE', 'JNB',
    'CPH', 'HEL', 'BRU', 'LIS', 'DUB', 'TLV', 'NZE', 'SET', 'KLS', 'IST', 'MEX', 'SAU',
]
SCREENER_EXCHANGES = US_SCREENER_EXCHANGES + INTERNATIONAL_EXCHANGES


def _usd_rate(exchange: str) -> float | None:
    """Latest local-currency -> USD rate for the exchange (1.0 for USD), or None if unknown."""
    currency = tu.currency_for_exchange(exchange)
    if currency == 'USD':
        return 1.0
    if not currency:
        return None
    today = date.today()
    rates = pricing.fetch_historic_usd_rates(currency, today - timedelta(days=10), today)
    if isinstance(rates, str) or not rates:
        return None
    return rates[max(rates)]


# Stage 1: pull the eligible large-cap universe from the FMP screener (today only — no historical mode).
def _fetch_all_screener_results() -> list[dict]:
    """
    One paginated screener call per exchange in SCREENER_EXCHANGES. FMP's screener compares
    marketCapMoreThan against each listing's *local-currency* market cap (verified: ¥10B lets
    in $65M companies, €10B misses $10–12B ones), so the $10B threshold is converted into each
    exchange's currency first. An exchange with no known currency/FX rate is skipped rather than
    queried with an unconverted threshold. Results are de-duped by (symbol, exchange).
    """
    seen: set[tuple[str | None, str | None]] = set()
    results: list[dict] = []

    for exchange in SCREENER_EXCHANGES:
        rate = _usd_rate(exchange)
        if rate is None:
            log.record_notice(f"Screener: no currency/FX rate for exchange {exchange} — skipped.")
            continue
        local_threshold = int(LARGE_CAP_THRESHOLD / rate)
        caps: list[float] = []
        for page in range(api_stocks.SCREENER_MAX_PAGES):
            page_data = api_stocks.fetch_company_screener(
                market_cap_more_than=local_threshold,
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
                if row.get('marketCap'):
                    caps.append(row['marketCap'])
            if len(page_data) < api_stocks.SCREENER_PAGE_LIMIT:
                break
        smallest = f"${min(caps) * rate / 1e9:,.1f}B" if caps else "n/a"
        log.record_status(f"Screener {exchange}: threshold {local_threshold:,} local, {len(caps)} companies, smallest {smallest}")

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
) -> dict[tuple[str, str], tuple[int, str, float]]:
    """
    Resolve each screener result to a ticker_id.
    Returns {(symbol, exchange): (ticker_id, exchange, market_cap)} — one entry per listing
    (a bare symbol isn't unique across exchanges: e.g. ANZ on ASX and NZE, or numeric codes).
    New tickers are upserted; existing ones get their market cap updated.

    A symbol can have rows on multiple exchanges — either already in the DB, or across
    multiple rows within this one screener call (e.g. the same company cross-listed).
    Each row is resolved against its own exact (symbol, exchange) pair rather than
    blindly reusing whatever ticker_id the symbol maps to, so a row's market cap never
    gets attached to the wrong exchange's ticker. Listings of the same company are collapsed
    later, by master ticker (_apply_master_consolidation), not here.

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
    resolved: dict[tuple[str, str], tuple[int, str, float]] = {}
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

        resolved[(symbol, exchange)] = (ticker_id, exchange, resolved_market_cap)

    return resolved


# Stage 2b: one row per company. FMP reports the whole company's market cap on every listing,
# so listings are never summed — the company's cap is its master's company_market_cap (taken
# from its primary listing by master.refresh_company_data), and its region is the company's.
def _apply_master_consolidation(
    resolved: dict[tuple[str, str], tuple[int, str, float]],
) -> list[tuple[int, str, float]]:
    """Returns [(company_ticker_id, region, market_cap_usd)], companies below the $10B floor dropped."""
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
        cid = t.master_ticker_id if (t and t.master_ticker_id in tickers_by_id) else tid
        if cid in out:
            continue
        c = tickers_by_id.get(cid)
        cap = (c.company_market_cap if c and c.company_market_cap else None) or market_cap
        region = (c.region if c else None) or (company.US if exchange in tu.US_LISTING_EXCHANGES else company.INTERNATIONAL)
        out[cid] = (cid, region, cap)

    kept = [row for row in out.values() if row[2] >= LARGE_CAP_THRESHOLD]
    if len(kept) < len(out):
        log.record_status(f"Dropped {len(out) - len(kept)} companies below the ${LARGE_CAP_THRESHOLD / 1e9:.0f}B USD floor.")
    return kept


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


# Stage 3: split companies into the two regions each blend benchmark covers, by the company's
# region (its primary listing — see modules/ticker/company.py), not by the listing's exchange.
def _split_us_intl(
    rows: list[tuple[int, str, float]],  # (company_ticker_id, region, market_cap)
) -> tuple[list[tuple[int, float]], list[tuple[int, float]]]:
    """Splits (ticker_id, region, market_cap) rows into (us_items, intl_items) pairs."""
    us_items   = [(tid, mc) for tid, region, mc in rows if region == company.US]
    intl_items = [(tid, mc) for tid, region, mc in rows if region != company.US]
    return us_items, intl_items


# Stage 3b: look up the two target Benchmark rows (region/cap/style config) to store into.
def _fetch_blend_benchmarks() -> tuple[benchmark.Benchmark | None, benchmark.Benchmark | None]:
    us_blend   = benchmark.fetch_by_region_and_style('US', 'blend')
    intl_blend = benchmark.fetch_by_region_and_style('International', 'blend')
    return us_blend, intl_blend


@dataclass
class BenchmarkRunStats:
    masters_updated: int      # master links added/changed by the in-run master sync
    unlinked: int             # stale master links removed by the in-run master sync
    company_caps: int         # company market caps refreshed by the in-run master sync
    us_companies: int
    intl_companies: int


def run() -> BenchmarkRunStats:
    """
    Fetch the large-cap universe from the FMP screener, register new tickers, run the master
    sync (the only one in the Wednesday cron — it has to follow the screener so today's new
    listings are linked to their companies), and store the US and International blend
    snapshots for today.

    Raises — so the cron stops before best ideas/funds — when a screener call still fails
    after its retries, or when either benchmark would come out empty (e.g. run on a day with
    no trading data, where no market cap can be validated).
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

        # Stage 2a: link any listings first seen today to their company and refresh company caps
        # and regions from today's validated values.
        masters_updated, unlinked, caps_updated = ticker_master.sync_masters_and_company_data()

        # Stage 2b: one row per company, at its company market cap, $10B USD floor applied.
        consolidated = _apply_master_consolidation(resolved)
        log.record_status(f"Consolidated to {len(consolidated)} companies.")

        # Stage 3: split into US / International and resolve the two blend Benchmark rows.
        holding_date = date.today()
        us_items, intl_items = _split_us_intl(consolidated)
        log.record_status(f"Split: {len(us_items)} US, {len(intl_items)} International.")
        if not us_items or not intl_items:
            raise Exception(
                f"Benchmark would be empty ({len(us_items)} US, {len(intl_items)} International companies) — "
                f"no market caps could be validated for {holding_date}. Existing snapshots left unchanged."
            )

        us_blend, intl_blend = _fetch_blend_benchmarks()

        # Stage 4: weight and store one snapshot per region for today's holding_date.
        if us_blend:
            _build_and_store(us_blend.id, us_blend.name, us_items, holding_date)
        if intl_blend:
            _build_and_store(intl_blend.id, intl_blend.name, intl_items, holding_date)

        batch_run.update_completed_at(batch_run_id)
        log.record_status("Benchmark Generator (blend) completed.\n")
        return BenchmarkRunStats(masters_updated, unlinked, caps_updated, len(us_items), len(intl_items))

    except Exception as e:
        log.record_error(f"Error in benchmark_generator blend: {e}")
        raise


