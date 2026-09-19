import log
from dataclasses import dataclass
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor

from modules.core import api_stocks
from modules.object import ticker
from modules.object.ticker_value import TickerValue, fetch_values_for_ticker, fetch_latest_value_date, upsert as _upsert_tv

HISTORY_LOOKBACK_DAYS = 21          # calendar days - comfortably covers 10+ trading days
                                    # even across a holiday-heavy stretch
PRICE_TOLERANCE       = 0.005      # 0.5% relative diff
MARKET_CAP_TOLERANCE  = 0.01       # 1.0% relative diff (looser: also depends on shares outstanding)
GRACE_PERIOD_DAYS     = 5          # consecutive calendar days a ticker may go unrecorded while
                                    # mismatching before it gets flagged invalid


@dataclass
class Mismatch:
    ticker_id: int
    value_date: date
    field: str              # "stock_price" | "market_cap"
    stored_value: float
    fetched_value: float
    pct_diff: float


def fetch_price_and_market_cap_history(symbol: str, start: date, end: date) -> dict[date, tuple[float, float]] | str:
    """Parallel-fetch historical price + market cap for `symbol` over [start, end].
    Returns {date: (price, market_cap)} for weekday dates present in BOTH series,
    or the underlying error string as-is if either call fails."""
    with ThreadPoolExecutor(max_workers=2) as executor:
        prices_future = executor.submit(api_stocks.get_stock_historic_prices, symbol, start, end)
        market_caps_future = executor.submit(api_stocks.get_stock_historic_market_cap, symbol, start, end)
        prices_raw = prices_future.result()
        market_caps_raw = market_caps_future.result()

    if isinstance(prices_raw, str):
        return prices_raw
    if isinstance(market_caps_raw, str):
        return market_caps_raw

    price_by_date = {date.fromisoformat(row["date"]): float(row["price"]) for row in prices_raw}
    market_cap_by_date = {date.fromisoformat(row["date"]): float(row["marketCap"]) for row in market_caps_raw}

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


def store_validated_ticker_value(ticker_id: int, symbol: str, value_date: date) -> TickerValue | None:
    try:
        fetched = fetch_price_and_market_cap_history(symbol, value_date - timedelta(days=HISTORY_LOOKBACK_DAYS), value_date)
        if isinstance(fetched, str):
            log.record_notice(f"Historic price/market cap unavailable for ticker_id={ticker_id} ({symbol}): {fetched}")
            return None

        if value_date not in fetched:
            log.record_notice(f"No historic price/market cap for ticker_id={ticker_id} ({symbol}) on {value_date}")
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
            else:
                log.record_notice(
                    f"Withholding ticker_value write for ticker_id={ticker_id} ({symbol}) on {value_date}: "
                    f"mismatch vs stored history ({days_since_last_good}/{GRACE_PERIOD_DAYS} grace days elapsed)."
                )
            return None

        price, market_cap = fetched[value_date]
        item = TickerValue(ticker_id=ticker_id, value_date=value_date, stock_price=price, market_cap=market_cap)
        _upsert_tv(item)
        return item

    except Exception as e:
        log.record_notice(f"Failed to store validated ticker_value for ticker_id={ticker_id} ({symbol}): {e}")
        return None
