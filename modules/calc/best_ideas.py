import pandas as pd
from modules.object.provider_etf_holding import ProviderEtfHolding
from modules.object.ticker_value import TickerValue

BASE_INDEX_LEVEL = 100.0

def find_best_ideas(
    holdings: list[ProviderEtfHolding],
    values: list[TickerValue],
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

    if limit is not None:
        if limit <= 0:
            raise ValueError("limit must be a positive integer")
        best_ideas = best_ideas.head(limit)

    return best_ideas.reset_index(drop=True)


def print_best_ideas(best_ideas_df):
    print(
        best_ideas_df[
            ["symbol", "etf_weight", "benchmark_weight", "delta"]
        ]
    )