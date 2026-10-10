import log
import re
from dataclasses import dataclass, field
from datetime import date, timedelta
from modules.core import api_stocks
from modules.object import batch_run, ticker, screener_listing
from modules.object.screener_listing import ScreenerListing, LINE_HOME, LINE_FOREIGN, LINE_NON_EQUITY, LINE_ORDER_BOOK
from modules.object.ticker import Ticker
from modules.ticker import company, index_funds, pricing
from modules.ticker import util as tu

# FMP's screener has to be queried per exchange to get real international coverage (an
# unfiltered call is heavily US/Canada-biased and mixes currencies), so the screen is the
# union of these exchanges' large caps.
US_SCREENER_EXCHANGES = ['NYSE', 'NASDAQ', 'AMEX']
INTERNATIONAL_EXCHANGES = [
    'TSX', 'LSE',
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


def screen_threshold(screen_date: date) -> float:
    """The USD market cap the screen starts at: the lowest large-cap cutoff of the benchmarks'
    markets (index_funds.large_cutoffs - a float-cap breakpoint; a company's whole cap is at least
    its float cap, so every company that can pass is screened) with a margin for the moves
    between the weekly breakpoints."""
    return min(index_funds.large_cutoffs(screen_date).values()) * index_funds.SCREEN_MARGIN


def _fetch_all_screener_results(min_cap_usd: float) -> list[dict]:
    """
    One paginated screener call per exchange in SCREENER_EXCHANGES. FMP's screener compares
    marketCapMoreThan against each listing's *local-currency* market cap (verified: ¥10B lets
    in $65M companies, €10B misses $10–12B ones), so the USD threshold is converted into each
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
        local_threshold = int(min_cap_usd / rate)
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


def row_symbol(symbol: str | None) -> str:
    """Screener symbol without its exchange suffix (TD.TO -> TD), as TickerResolver stores it."""
    return re.split(r'[\s.]', symbol or '')[0]


def describe(listing: ScreenerListing) -> str:
    return f"{listing.symbol} [{listing.company_name}] ({listing.country or '?'})"


def _classify(symbol: str, exchange: str, name: str, country: str | None) -> str:
    """
    FMP's screener returns listings, not companies, and reports the whole company's cap on most
    of them — foreign lines, depositary receipts, even preferred shares and notes — so every
    line that isn't the company's own gets it counted again. The universal rule: a company
    enters through its home-market ordinary listing.
      - LINE_NON_EQUITY: preferred / note / warrant / unit / participation lines — never used.
      - LINE_ORDER_BOOK: LSE International Order Book mirrors (0Q16 = Bank of America), carrying
        the issuer's home-market data — never used.
      - LINE_HOME: the domicile's home market (util.market_tier 0/1), a US exchange (US-listed
        foreign companies such as Linde; ADRs are merged into their company by the master sync
        or dropped by the company builder's duplicate guard), or any exchange for a company domiciled
        where we screen no exchange (Bermuda, Cayman, Hungary, …) — registered here.
      - LINE_FOREIGN: a line outside the company's home market (Exxon on XETRA, Cisco's Canadian
        depositary receipt, Toyota mirrored on LSE) — admitted by the company builder only when
        it duplicates no company already in, which needs the master sync first.
    """
    bare = row_symbol(symbol)
    if company.is_non_equity_line(bare, name, exchange):
        return LINE_NON_EQUITY
    if tu.is_iob_line(bare, exchange):
        return LINE_ORDER_BOOK
    if tu.market_tier(country, bare, exchange) <= 2 or not tu.has_screened_home(country, SCREENER_EXCHANGES):
        return LINE_HOME
    return LINE_FOREIGN


def _fill_new_ticker_profile(symbol: str, exchange: str, full_symbol: str) -> None:
    """A listing first seen in the screener gets its FMP profile right away — its currency (a
    market doesn't always report in its exchange's currency), ISIN and CIK (so the master sync
    can link it) — instead of a week later from the profile refresh."""
    profile = api_stocks.get_stock_profile(full_symbol)
    if not isinstance(profile, dict):
        return
    ticker.upsert_by_symbol(Ticker(
        symbol=symbol, exchange=exchange,
        isin=profile.get('isin') or None, cusip=profile.get('cusip') or None, cik=profile.get('cik') or None,
        name=profile.get('companyName') or None, industry=profile.get('industry') or None,
        sector=profile.get('sector') or None, country=profile.get('country') or None,
        currency=profile.get('currency') or None, source='fmp', is_actively_trading=True,
        average_turnover=tu.profile_turnover(profile),
    ))


def _upsert_ticker(symbol: str, exchange: str, name: str, country: str, full_symbol: str) -> int | None:
    """Registers/updates the (symbol, exchange) row with screener data. Returns its ticker_id."""
    t = Ticker(
        symbol=symbol,
        exchange=exchange,
        name=name,
        country=country or None,
        source='fmp',
        is_actively_trading=True,
    )
    try:
        ticker_id, is_new = ticker.upsert_by_symbol(t)
    except Exception as e:
        log.record_notice(f"Failed to upsert ticker {symbol} ({exchange}): {e}")
        return None
    if is_new:
        _fill_new_ticker_profile(symbol, exchange, full_symbol)
    return ticker_id


def register_listings(listings: list[ScreenerListing], save: bool = True) -> None:
    """
    Registers each listing as a ticker (new ones with their FMP profile), setting its ticker_id;
    with `save`, the ticker_id is also saved on the stored line. Used by this step for the
    home-market lines and by the company builder for the foreign lines it admits. Values come
    from the valuation pass (modules/ticker/valuation.py), which values a stored screen's lines.

    A symbol can have rows on multiple exchanges — either already in the DB, or across
    multiple rows within one screen (e.g. the same company cross-listed). Each row is resolved
    against its own exact (symbol, exchange) pair rather than blindly reusing whatever
    ticker_id the symbol maps to, so a row's market cap never gets attached to the wrong
    exchange's ticker. Listings of the same company are grouped by the master sync, not here.
    """
    symbol_cache = ticker.fetch_all_for_symbol_cache()
    registered: dict[date, list[ScreenerListing]] = {}

    for listing in listings:
        if not listing.market_cap or listing.market_cap <= 0:
            continue
        exchange, name, country = listing.exchange, listing.company_name or '', listing.country or ''
        # Strip the exchange suffix (TD.TO -> TD) to match how TickerResolver stores symbols.
        symbol = row_symbol(listing.symbol)

        if symbol not in symbol_cache:
            # Brand new symbol — never seen before.
            ticker_id = _upsert_ticker(symbol, exchange, name, country, listing.symbol)
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
                ticker_id = _upsert_ticker(symbol, exchange, name, country, listing.symbol)
                if ticker_id is None:
                    continue
                if ticker.is_preferred_exchange(exchange, cached_exchange):
                    symbol_cache[symbol] = (ticker_id, exchange)

        listing.ticker_id = ticker_id
        registered.setdefault(listing.screen_date, []).append(listing)

    if save:
        for screen_date, items in registered.items():
            screener_listing.set_ticker_ids(screen_date, items)


@dataclass
class ScreenerRunStats:
    screen_date: date
    stored: bool = False                                 # screen stored in screener_listing (store=True)
    screened: int = 0
    home: int = 0
    foreign: int = 0
    order_book: int = 0
    non_equity: list[str] = field(default_factory=list)  # described lines, for reports
    not_trading: int = 0                                 # listings FMP calls inactive, not ours: left out
    registered: int = 0                                  # home lines registered as tickers


def summary(stats: ScreenerRunStats) -> str:
    """The cron email's section: a title, then one short bullet per fact."""
    return "\n".join([
        f"Screener {stats.screen_date} ({'stored' if stats.stored else 'registration only'})",
        f"- {stats.screened:,} listings",
        f"- {stats.registered:,} of {stats.home:,} home-market lines registered",
        f"- {stats.foreign:,} foreign lines {'stored for the company builder' if stats.stored else 'not used'}",
        f"- {len(stats.non_equity):,} non-equity lines skipped",
        f"- {stats.order_book:,} order-book lines skipped",
        f"- {stats.not_trading:,} listings FMP marks not trading skipped",
    ])


def run(screen_date: date | None = None, store: bool = False) -> ScreenerRunStats:
    """
    Screens FMP's large caps (every line classified, _classify) and registers the home-market
    lines as tickers, so the daily ticker maintenance covers every listing the generators
    will use. With `store` (the generation day, the sim, by hand) the screen is also stored in
    screener_listing — foreign lines included, decided by the company builder after the master
    sync — and the valuation pass (modules/ticker/valuation.py) then values its registered lines
    for screen_date. Registering takes about a minute; valuing the screen is what's weekly.

    screen_date defaults to the latest completed trading day (pricing.latest_value_date — the
    date every value is stored under). The sim passes its data cutoff instead.

    A listing FMP marks not actively trading (~10% of the screen, mostly long gone: Pioneer,
    VMware, Twitter) is kept only when it's a valid ticker of ours - one whose daily prices
    still show trading (resolver.profile_invalid_reason: Energy Transfer, Dillard's) - so the
    dead ones aren't registered.

    Raises — so the cron stops before the master sync and the generators — when a screener call
    still fails after its retries.
    """
    screen_date = screen_date or pricing.latest_value_date()
    batch_run_id = batch_run.insert(batch_run.BatchRun(process='screener', activation='auto'))
    log.record_status(f"Starting Screener batch job ID {batch_run_id} for {screen_date} ({'stored' if store else 'registration only'})")
    try:
        stats = ScreenerRunStats(screen_date=screen_date, stored=store)
        listings: list[ScreenerListing] = []
        min_cap = screen_threshold(screen_date)
        log.record_status(f"Screening from ${min_cap / 1e9:,.1f}B (the lowest benchmark cutoff x {index_funds.SCREEN_MARGIN:g}).")
        valid = {(t.symbol, t.exchange) for t in ticker.fetch_all() if not t.invalid}
        for row in _fetch_all_screener_results(min_cap):
            symbol = row.get('symbol')
            if not symbol:
                continue
            exchange = row.get('exchangeShortName') or ''
            if row.get('isActivelyTrading') is False and (row_symbol(symbol), exchange) not in valid:
                stats.not_trading += 1
                continue
            name, country = row.get('companyName') or '', row.get('country') or None
            listings.append(ScreenerListing(
                screen_date=screen_date, symbol=symbol, exchange=exchange,
                company_name=name or None, country=country,
                market_cap=row.get('marketCap'), price=row.get('price'),
                line_type=_classify(symbol, exchange, name, country),
            ))
        if store:
            screener_listing.replace_for_date(screen_date, listings)

        stats.screened = len(listings)
        home = [l for l in listings if l.line_type == LINE_HOME]
        stats.home = len(home)
        stats.foreign = sum(1 for l in listings if l.line_type == LINE_FOREIGN)
        stats.order_book = sum(1 for l in listings if l.line_type == LINE_ORDER_BOOK)
        stats.non_equity = [describe(l) for l in listings if l.line_type == LINE_NON_EQUITY]
        log.record_status(
            f"{'Stored' if store else 'Fetched'} {stats.screened} large-cap listings from the FMP screener: {stats.home} home-market, "
            f"{stats.foreign} foreign, {len(stats.non_equity)} non-equity, {stats.order_book} order-book."
        )

        register_listings(home, save=store)
        stats.registered = sum(1 for l in home if l.ticker_id)
        log.record_status(f"Registered {stats.registered} home-market listings.")

        batch_run.update_completed_at(batch_run_id)
        log.record_status("Screener completed.\n")
        return stats

    except Exception as e:
        log.record_error(f"Error in screener: {e}")
        raise
