import log
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date
from modules.object import batch_run, ticker, ticker_value, screener_listing, provider_etf_holding
from modules.object.screener_listing import ScreenerListing
from modules.object.ticker import Ticker
from modules.ticker import pricing
from modules.ticker.resolver import TickerResolver

WORKERS = 5                  # tickers valued in parallel — FMP's rate limit (200/min) is the bound
HOLDINGS_LOOK_BACK_DAYS = 7  # holdings best ideas can still use (best_ideas_generator.LOOK_BACK_WINDOW)


@dataclass
class ValueTarget:
    ticker_id: int
    full_symbol: str                        # FMP's symbol, with its exchange suffix (TD.TO)
    exchange: str | None
    currency: str | None                    # FMP's currency for the listing (ticker.currency)
    reference_shares: float | None = None   # share count of FMP's live quote — the glitch filter's reference
    verified_shares: float | None = None    # ticker.verified_shares — repairs a history on a wrong share count


@dataclass
class ValuationStats:
    value_date: date
    held: int = 0            # tickers in the ETFs' recent holdings
    screened: int = 0        # registered lines of the screen stored for value_date
    targets: int = 0         # both, once per ticker
    already_valued: int = 0  # a value already stored for value_date
    validated: int = 0       # validated and stored now
    withheld: int = 0        # no data for the date, a mismatch in its grace period, or flagged invalid


def summary(stats: ValuationStats) -> str:
    """One line for the cron email."""
    return (
        f"Ticker values {stats.value_date}: {stats.targets} tickers ({stats.held} held by ETFs, {stats.screened} "
        f"from the stored screen) — {stats.validated} validated, {stats.already_valued} already valued, "
        f"{stats.withheld} withheld"
    )


def targets_for_tickers(tickers: list[Ticker]) -> list[ValueTarget]:
    symbols = TickerResolver(TickerResolver.POPULATE_TICKER)
    return [ValueTarget(t.id, symbols.get_full_symbol(t), t.exchange, t.currency, verified_shares=t.verified_shares)
            for t in tickers if not t.invalid]


def targets_for_listings(listings: list[ScreenerListing]) -> list[ValueTarget]:
    """Registered screener lines, valued under the screener's own symbol with its quote's share count."""
    tickers_by_id = {t.id: t for t in ticker.fetch_by_ids([l.ticker_id for l in listings if l.ticker_id])}
    return [
        ValueTarget(l.ticker_id, l.symbol, l.exchange, tickers_by_id[l.ticker_id].currency, l.quote_shares,
                    tickers_by_id[l.ticker_id].verified_shares)
        for l in listings
        if l.ticker_id in tickers_by_id and not tickers_by_id[l.ticker_id].invalid
    ]


def targets_in_use(value_date: date, stats: ValuationStats | None = None) -> list[ValueTarget]:
    """Every ticker the generators use, once: the valid tickers in each active ETF's latest
    holdings from the last HOLDINGS_LOOK_BACK_DAYS, and the registered lines of the screen stored
    for value_date (Wednesdays) — whose target wins, for its quote's share count."""
    held = targets_for_tickers(ticker.fetch_by_ids(provider_etf_holding.fetch_valid_ticker_ids_in_recent_holdings(HOLDINGS_LOOK_BACK_DAYS)))
    screened = targets_for_listings(screener_listing.fetch_for_date(value_date))
    by_id = {t.ticker_id: t for t in held}
    by_id.update({t.ticker_id: t for t in screened})
    if stats is not None:
        stats.held, stats.screened, stats.targets = len(held), len(screened), len(by_id)
    return list(by_id.values())


def store_values(targets: list[ValueTarget], value_date: date, stats: ValuationStats | None = None) -> ValuationStats:
    """
    Stores each target's validated price and market cap (pricing.store_validated_ticker_value:
    checked against FMP's history, glitches repaired, converted to USD) for value_date, in
    parallel — skipping those already valued for that date. A withheld value leaves the ticker
    without one for the date; it's flagged invalid only after its grace period.
    """
    stats = stats or ValuationStats(value_date=value_date, targets=len(targets))
    stored = ticker_value.fetch_market_caps_between([t.ticker_id for t in targets], value_date, value_date)
    todo = [t for t in targets if t.ticker_id not in stored]
    stats.already_valued += len(targets) - len(todo)

    def value(t: ValueTarget) -> bool:
        v = pricing.store_validated_ticker_value(
            t.ticker_id, t.full_symbol, value_date, exchange=t.exchange, currency=t.currency,
            reference_shares=t.reference_shares, verified_shares=t.verified_shares)
        return v is not None and v.market_cap is not None

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        results = list(executor.map(value, todo))
    stats.validated += sum(results)
    stats.withheld += len(results) - sum(results)
    return stats


def run(value_date: date | None = None) -> ValuationStats:
    """
    The daily valuation pass: every ticker in use (targets_in_use) gets its validated value for
    value_date — by default the latest completed trading day (pricing.latest_value_date), the
    date the whole pipeline stores and reads values under.
    """
    value_date = value_date or pricing.latest_value_date()
    batch_run_id = batch_run.insert(batch_run.BatchRun(process='ticker_values', activation='auto'))
    log.record_status(f"Starting Ticker Values batch job ID {batch_run_id} for {value_date}")
    try:
        stats = ValuationStats(value_date=value_date)
        store_values(targets_in_use(value_date, stats), value_date, stats)
        batch_run.update_completed_at(batch_run_id)
        log.record_status(summary(stats) + "\n")
        return stats
    except Exception as e:
        log.record_error(f"Error in ticker values: {e}")
        raise
