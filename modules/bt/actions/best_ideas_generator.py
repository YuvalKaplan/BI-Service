import log
import pandas as pd
from datetime import date
from modules.bt.object import provider, provider_etf
from modules.bt.object.provider_etf_holding import ProviderEtfHolding, fetch_holding_dates_available_past_week, fetch_valid_holdings_by_provider_etf_id
from modules.bt.object.ticker_value import TickerValue, fetch_price_dates_available_past_week, fetch_tickers_by_symbols_on_date
from modules.bt.object import best_idea
from modules.bt.object.ticker_value import TickerValue

MIN_HOLDINGS = 10
MAX_DATE_DIFF_DAYS = 5
MAX_BEST_IDEAS_PER_FUND = 3
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

def record_problem(etf_id: int, error: str, message: str | None, problem_etfs: list[str]):
    p = provider.fetch_by_etf_id(etf_id)
    etf = provider_etf.fetch_by_id(etf_id)
    item_info = f"[Provider: '{p.name}' ({p.id}), ETF: '{etf.name}' ({etf_id})]"
    record = f"{item_info}\t{error}\t{message or ''}"

    log.record_status(record)
    problem_etfs.append(record)

def run(etf_ids: list[int], today: date) -> tuple[int, int, list[str]]:
    try:
        log.record_status(f"Running Best Ideas Generator - will proccess {len(etf_ids)} ETFs.")

        total_etfs = len(etf_ids)
        generated_etfs = 0
        problem_etfs: list[str] = []
        available_price_dates = fetch_price_dates_available_past_week(today)
        
        for etf_id in etf_ids:
            try:
                available_holding_dates = fetch_holding_dates_available_past_week(etf_id, today)
                if len(available_holding_dates) == 0:
                    record_problem(etf_id=etf_id, error="No holdings have been downloaded for the past week", message=None, problem_etfs=problem_etfs)
                    continue

                latest_common_date = max(set(available_price_dates) & set(available_holding_dates), default=None)

                if latest_common_date is None:
                    message = f"Latest holdings date: {max(available_holding_dates).strftime("%b %d, %Y")}, Latest price date: {max(available_price_dates).strftime("%b %d, %Y")}"
                    record_problem(etf_id=etf_id, error="Data sources out of sync", message=message, problem_etfs=problem_etfs)
                    continue

                holdings = fetch_valid_holdings_by_provider_etf_id(etf_id, latest_common_date)
                if len(holdings) < MIN_HOLDINGS:
                    if len(holdings) == 0:
                        error = f"No holdings"
                    else:
                        error = f"Less than {MIN_HOLDINGS} holdings"
                    record_problem(etf_id=etf_id, error=error, message=None, problem_etfs=problem_etfs)
                    continue
                
                tickers = [h.ticker for h in holdings]
                values = fetch_tickers_by_symbols_on_date(tickers, latest_common_date)

                if len(values) < int(0.9 * len(holdings)):
                    message = f"Holdings: {len(holdings)}, Values: {len(values)} - on date {latest_common_date.strftime("%b %d, %Y")}"
                    record_problem(etf_id=etf_id, error="Too many prices missing (less than 90 percent of holdngs)", message=message, problem_etfs=problem_etfs)
                    continue

                best_ideas_df = find_best_ideas(holdings, values, MAX_BEST_IDEAS_PER_FUND)
                rows = best_idea.df_to_rows(
                    best_ideas_df,
                    provider_etf_id=etf_id,
                    value_date=latest_common_date
                )
                best_idea.insert_bulk(rows)
                generated_etfs += 1
            except Exception as e:
                record_problem(etf_id=etf_id, error=f"{e}", message=None, problem_etfs=problem_etfs)

        log.record_status(f"Finished Best Ideas Generator batch run on {total_etfs} etfs.\n")
        return total_etfs, generated_etfs, problem_etfs
    
    except Exception as e:
        log.record_error(f"Error in best ideas generator batch run: {e}")
        raise e