import log
from dataclasses import dataclass
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor

from modules.core import api_stocks
from modules.object import ticker
from modules.object.ticker_value import TickerValue, fetch_values_for_ticker, fetch_latest_value_date, upsert as _upsert_tv
from modules.ticker import util as tu

HISTORY_LOOKBACK_DAYS = 21          # calendar days - comfortably covers 10+ trading days
                                    # even across a holiday-heavy stretch
PRICE_TOLERANCE       = 0.005      # 0.5% relative diff
MARKET_CAP_TOLERANCE  = 0.01       # 1.0% relative diff (looser: also depends on shares outstanding)
GRACE_PERIOD_DAYS     = 5          # consecutive calendar days a ticker may go unrecorded while
                                    # mismatching before it gets flagged invalid

_fx_history_cache: dict[tuple[str, date, date], dict[date, float]] = {}


def fetch_historic_usd_rates(currency: str | None, start: date, end: date) -> dict[date, float] | str:
    """
    Cached {date: rate_to_usd} for every day FMP has a quote for `currency`->USD over
    [start, end]. Using the actual per-date historical rate (not a single "spot" rate applied
    blanket across a whole range) matters here — FX rates move meaningfully over time (e.g.
    KRWUSD moved ~6% between 2026-01-01 and 2026-09-23), so a value from January needs
    January's rate, not today's.

    Returns {} (not an error) when currency is None/USD, since no conversion is needed then —
    callers should treat an empty dict as "use the raw value as-is". Returns an error string if
    the currency is unrecognized or the fetch fails; callers should treat that the same as any
    other "data unavailable" case (skip/withhold) rather than store an unconverted, wrong-unit
    value. Cached per (currency, start, end) since many tickers on the same exchange typically
    share both currency and requested date range within a single run.
    """
    if not currency or currency == 'USD':
        return {}
    key = (currency, start, end)
    if key in _fx_history_cache:
        return _fx_history_cache[key]
    raw = api_stocks.get_historic_fx_rates(currency, 'USD', start, end)
    if isinstance(raw, str):
        log.record_notice(f"Historic FX rates unavailable for {currency}->USD: {raw}")
        return raw
    rates: dict[date, float] = {}
    for row in raw:
        try:
            rates[date.fromisoformat(row["date"])] = float(row["price"])
        except (KeyError, ValueError, TypeError):
            continue
    _fx_history_cache[key] = rates
    return rates


def closest_value_for_date(series: dict[date, float], target: date, window_days: int = 5) -> float | None:
    """Nearest available value to `target` within `series`, searching outward day-by-day up to
    window_days in either direction — a data series doesn't always share the same trading-day
    calendar as whatever it's being matched against (e.g. FX markets trade on different
    holidays than a given stock exchange)."""
    for offset in range(window_days + 1):
        for d in (target - timedelta(days=offset), target + timedelta(days=offset)):
            if d in series:
                return series[d]
    return None


@dataclass
class Mismatch:
    ticker_id: int
    value_date: date
    field: str              # "stock_price" | "market_cap"
    stored_value: float
    fetched_value: float
    pct_diff: float


def fetch_price_and_market_cap_history(
    symbol: str, start: date, end: date, currency: str | None = None,
) -> dict[date, tuple[float, float]] | str:
    """Parallel-fetch historical price + market cap for `symbol` over [start, end].
    Returns {date: (price, market_cap)} for weekday dates present in BOTH series,
    or the underlying error string as-is if either call fails.

    `currency` is the currency `symbol` is quoted in (see modules.ticker.util.EXCHANGE_CURRENCY)
    — market_cap is converted to USD before being returned, using that date's own historical
    FX rate (not a single blanket rate), since FMP reports it in the security's native currency
    and this is stored/compared against a USD threshold everywhere downstream. If currency is
    None/USD, no conversion is applied. If the FX rate series can't be resolved at all, this
    returns an error string (same as an unavailable price/market-cap response) so the caller
    skips/withholds rather than storing a wrong-unit value; a date with no matching FX rate
    within the lookup window is simply dropped rather than stored unconverted."""
    with ThreadPoolExecutor(max_workers=2) as executor:
        prices_future = executor.submit(api_stocks.get_symbol_historic_prices, symbol, start, end)
        market_caps_future = executor.submit(api_stocks.get_stock_historic_market_cap, symbol, start, end)
        prices_raw = prices_future.result()
        market_caps_raw = market_caps_future.result()

    if isinstance(prices_raw, str):
        return prices_raw
    if isinstance(market_caps_raw, str):
        return market_caps_raw

    fx_rates = fetch_historic_usd_rates(currency, start, end)
    if isinstance(fx_rates, str):
        return fx_rates

    price_by_date = {date.fromisoformat(row["date"]): float(row["price"]) for row in prices_raw}

    market_cap_by_date: dict[date, float] = {}
    for row in market_caps_raw:
        d = date.fromisoformat(row["date"])
        raw_mc = float(row["marketCap"])
        if not fx_rates:  # currency is None/USD - no conversion needed
            market_cap_by_date[d] = raw_mc
            continue
        rate = closest_value_for_date(fx_rates, d)
        if rate is None:
            continue  # no FX rate close enough to this date to trust - drop it, don't guess
        market_cap_by_date[d] = raw_mc * rate

    common_dates = price_by_date.keys() & market_cap_by_date.keys()
    return {
        d: (price_by_date[d], market_cap_by_date[d])
        for d in common_dates
        if d.weekday() < 5
    }


def _relative_diff(a: float, b: float) -> float:
    denom = max(abs(a), abs(b), 1e-9)
    return abs(a - b) / denom


def compare_overlap(ticker_id: int, fetched: dict[date, tuple[float, float]], stored: list[TickerValue]) -> list[Mismatch]:
    """Compares each `stored` row whose value_date is also a key in `fetched` against the
    freshly-fetched value. One Mismatch per (date, field) exceeding tolerance."""
    mismatches: list[Mismatch] = []
    for row in stored:
        if row.value_date not in fetched:
            continue
        fetched_price, fetched_market_cap = fetched[row.value_date]

        if row.stock_price is not None:
            pct_diff = _relative_diff(row.stock_price, fetched_price)
            if pct_diff > PRICE_TOLERANCE:
                mismatches.append(Mismatch(
                    ticker_id=ticker_id, value_date=row.value_date, field="stock_price",
                    stored_value=row.stock_price, fetched_value=fetched_price, pct_diff=pct_diff,
                ))

        if row.market_cap is not None:
            pct_diff = _relative_diff(row.market_cap, fetched_market_cap)
            if pct_diff > MARKET_CAP_TOLERANCE:
                mismatches.append(Mismatch(
                    ticker_id=ticker_id, value_date=row.value_date, field="market_cap",
                    stored_value=row.market_cap, fetched_value=fetched_market_cap, pct_diff=pct_diff,
                ))

    return mismatches


def build_mismatch_reason(mismatches: list[Mismatch]) -> str:
    parts = [
        f"{m.value_date} {m.field}: stored={m.stored_value:.4g} fetched={m.fetched_value:.4g} ({m.pct_diff:.1%} diff)"
        for m in sorted(mismatches, key=lambda m: m.value_date)[:5]
    ]
    suffix = f" (+{len(mismatches) - 5} more)" if len(mismatches) > 5 else ""
    return "Price/market cap mismatch vs FMP history on " + "; ".join(parts) + suffix


def store_validated_ticker_value(ticker_id: int, symbol: str, value_date: date, exchange: str | None = None) -> TickerValue | None:
    """`exchange` (the exchange `symbol` trades on) determines which currency market_cap gets
    converted from — see modules.ticker.util.EXCHANGE_CURRENCY. Omit it only for callers that
    can't know the exchange (market_cap will then be stored unconverted, native-currency)."""
    try:
        currency = tu.currency_for_exchange(exchange)
        fetched = fetch_price_and_market_cap_history(
            symbol, value_date - timedelta(days=HISTORY_LOOKBACK_DAYS), value_date, currency=currency,
        )
        if isinstance(fetched, str):
            # Not logged: no data available for this ticker/date is a routine, expected
            # outcome (not every ticker has fresh data every run) — only a ticker actually
            # being marked invalid is worth a log entry, see below.
            return None

        if value_date not in fetched:
            return None

        stored = fetch_values_for_ticker(
            ticker_id,
            value_date - timedelta(days=HISTORY_LOOKBACK_DAYS),
            value_date - timedelta(days=1),
        )
        mismatches = compare_overlap(ticker_id, fetched, stored)

        if mismatches:
            last_good_date = fetch_latest_value_date(ticker_id)
            days_since_last_good = (value_date - last_good_date).days if last_good_date else GRACE_PERIOD_DAYS
            if days_since_last_good >= GRACE_PERIOD_DAYS:
                reason = build_mismatch_reason(mismatches)
                ticker.update_invalid(ticker_id, reason)
                log.record_notice(f"Flagged ticker_id={ticker_id} ({symbol}) invalid: {reason}")
            # else: still within its grace period - withheld silently, not logged (routine).
            return None

        price, market_cap = fetched[value_date]
        item = TickerValue(ticker_id=ticker_id, value_date=value_date, stock_price=price, market_cap=market_cap)
        _upsert_tv(item)
        return item

    except Exception as e:
        log.record_notice(f"Failed to store validated ticker_value for ticker_id={ticker_id} ({symbol}): {e}")
        return None
