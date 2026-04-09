import log
import pandas as pd
from dataclasses import dataclass
from datetime import date

HOLDING_DELTA_LIMIT_DROP_OFF = 0.25


@dataclass
class HoldingItem:
    ticker: str | None
    shares: float | None


@dataclass
class TickerValueItem:
    symbol: str | None
    stock_price: float | None
    market_cap: float | None


def to_holding_item(h) -> HoldingItem:
    """Convert any provider_etf_holding dataclass (live or BT) to the shared HoldingItem used by find_best_ideas."""
    return HoldingItem(ticker=h.ticker, shares=h.shares)


def to_ticker_value_item(v) -> TickerValueItem:
    """Convert any ticker_value dataclass (live or BT) to the shared TickerValueItem used by find_best_ideas."""
    return TickerValueItem(symbol=v.symbol, stock_price=v.stock_price, market_cap=v.market_cap)


def find_best_ideas(
    holdings: list[HoldingItem],
    values: list[TickerValueItem],
    limit: int | None = None
) -> pd.DataFrame:
    """
        Compare the delta between a virtual market-cap based benchmark and the real invested amount (stocks * price).
        Extract the holdings on which the manager has a positive conviction.
    """
    # --- Holdings DataFrame ---
    df_holdings = pd.DataFrame(
        {
            "symbol": h.ticker,
            "shares": h.shares
        }
        for h in holdings
        if h.ticker and h.shares and h.shares > 0
    )

    # --- Prices / market caps ---
    df_values = pd.DataFrame(
        {
            "symbol": v.symbol,
            "price": v.stock_price,
            "market_cap": v.market_cap
        }
        for v in values
        if v.symbol and v.stock_price and v.market_cap
    )

    # --- Merge ---
    df = df_holdings.merge(df_values, on="symbol", how="inner")

    if df.empty:
        raise ValueError("No overlapping symbols between holdings and values")

    # --- ETF weights ---
    df["etf_value"] = df["shares"] * df["price"]
    total_etf_value = df["etf_value"].sum()
    df["etf_weight"] = df["etf_value"] / total_etf_value

    # --- Benchmark weights ---
    total_market_cap = df["market_cap"].sum()
    df["benchmark_weight"] = df["market_cap"] / total_market_cap

    # --- Active weight (manager conviction) ---
    df["delta"] = df["etf_weight"] - df["benchmark_weight"]

    # --- Best ideas = overweight positions ---
    best_ideas = (
        df[df["delta"] > 0]
        .sort_values("delta", ascending=False)
    )

    # Drop symbols where delta exceeds the defined limit
    dropped = best_ideas[best_ideas["delta"] > HOLDING_DELTA_LIMIT_DROP_OFF]
    if not dropped.empty:
        dropped_symbols = dropped["symbol"].tolist()
        log.record_status(f"Dropping {len(dropped_symbols)} best-idea(s) above delta limit {HOLDING_DELTA_LIMIT_DROP_OFF}: {dropped_symbols}")
    best_ideas = best_ideas[best_ideas["delta"] <= HOLDING_DELTA_LIMIT_DROP_OFF]

    if limit is not None:
        if limit <= 0:
            raise ValueError("limit must be a positive integer")
        best_ideas = best_ideas.head(limit)

    return best_ideas.reset_index(drop=True)


def find_latest_common_date(holding_dates: list[date], price_dates: list[date]) -> date | None:
    """Return the latest date present in both holding and price date sets, or None if no overlap."""
    common = set(holding_dates) & set(price_dates)
    return max(common) if common else None


def filter_stale_holdings(
    holdings: list[HoldingItem],
    last_price_dates: dict[str, date | None],
    as_of_date: date,
    days_threshold: int,
) -> tuple[list[HoldingItem], list[str]]:
    """
    Split holdings into valid (recently priced) and stale (no price within days_threshold of as_of_date).
    Returns (valid_holdings, stale_ticker_symbols).
    """
    valid: list[HoldingItem] = []
    stale: list[str] = []
    for h in holdings:
        if h.ticker is None:
            continue
        last_date = last_price_dates.get(h.ticker)
        if last_date and (as_of_date - last_date).days <= days_threshold:
            valid.append(h)
        else:
            stale.append(h.ticker)
    return valid, stale


def print_best_ideas(best_ideas_df):
    print(
        best_ideas_df[
            ["symbol", "etf_weight", "benchmark_weight", "delta"]
        ]
    )
