import log
import pandas as pd
from datetime import date
from modules.bt.object import provider, provider_etf
from modules.bt.object.provider_etf_holding import ProviderEtfHolding, fetch_holding_dates_available_past_period, fetch_valid_holdings_by_provider_etf_id
from modules.bt.object.ticker_value import TickerValue, fetch_latest_price_date_for_ticker, fetch_tickers_by_symbols_on_date
from modules.bt.object import best_idea
from modules.bt.object.ticker_value import TickerValue

# Configuration constants
DAYS_NO_PRICING = 7 # Threshold for stale holdings
MIN_HOLDINGS_WITH_PRICES_PCT=0.9
LOOK_BACK_FOR_COMMON_DATE=14
MAX_BEST_IDEAS_PER_FUND = 10

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
    record = f"{item_info} - {error}. {message or ''}"

    log.record_status(record)
    problem_etfs.append(record)


def run(etf_ids: list[int], today: date) -> tuple[int, int, list[str]]:

    successes :list[str] = []
    try:
        log.record_status(f"Running Best Ideas Generator - processing {len(etf_ids)} ETFs.")
        total_etfs, processed_etfs, problem_etfs = len(etf_ids), 0, []

        for etf_id in etf_ids:
            try:
                # 1. Get the most recent holding date on or prior to 'today'
                available_holding_dates = fetch_holding_dates_available_past_period(
                    etf_id, today, LOOK_BACK_FOR_COMMON_DATE
                )
                if not available_holding_dates:
                    record_problem(etf_id=etf_id, error=f"No holdings have been downloaded for the past {LOOK_BACK_FOR_COMMON_DATE} days", message=None, problem_etfs=problem_etfs)
                    continue
                
                target_date = max(available_holding_dates)
                if target_date.weekday() >= 5:
                    record_problem(etf_id=etf_id, error=f"No prices on weekends", message=f"Last available price date {target_date}", problem_etfs=problem_etfs)
                    continue

                holdings = fetch_valid_holdings_by_provider_etf_id(etf_id, target_date)
                
                # 2. Preemptively remove holdings with no pricing in the last DAYS_NO_PRICING
                # This check ensures we don't fail an ETF just because of "dead" tickers
                valid_tickers_for_etf = []
                for h in holdings:
                    
                    # Logic assumes fetch_latest_price_date_for_ticker(h.ticker) exists or similar
                    last_price_date = fetch_latest_price_date_for_ticker(h.ticker, target_date)
                    if last_price_date and (target_date - last_price_date).days <= DAYS_NO_PRICING:
                        valid_tickers_for_etf.append(h)
                    else:
                        record_problem(etf_id=etf_id, error=f"STALE HOLDING", message=f"The ETF holding {h.ticker} has no prices for over {DAYS_NO_PRICING} days - will not be included in best ideas", problem_etfs=problem_etfs)
                
                # Update current holdings to only include non-stale tickers
                current_holdings = valid_tickers_for_etf
                ticker_symbols = [h.ticker for h in current_holdings]

                if len(ticker_symbols) == 0:
                    record_problem(etf_id=etf_id, error=f"No holdings with matching prices", message=None, problem_etfs=problem_etfs)
                    continue

                # 3. Fetch prices for EXACT target_date
                values = fetch_tickers_by_symbols_on_date(ticker_symbols, target_date)
                
                # 4. Percentage Threshold Case
                # We filter current_holdings down to only those that actually returned a value for the target_date
                priced_symbols = {v.symbol for v in values}
                final_holdings_to_process = [h for h in current_holdings if h.ticker in priced_symbols]
                
                coverage_ratio = len(final_holdings_to_process) / len(current_holdings)

                if coverage_ratio < MIN_HOLDINGS_WITH_PRICES_PCT:
                    missing_count = len(current_holdings) - len(final_holdings_to_process)
                    msg = f"Coverage {coverage_ratio:.1%}. Missing {missing_count} prices for date {target_date.strftime('%Y-%m-%d')}"
                    record_problem(etf_id=etf_id, error="Insufficient pricing coverage", message=msg, problem_etfs=problem_etfs)
                    continue

                # 5. Generate and batch insert
                best_ideas_df = find_best_ideas(current_holdings, values, MAX_BEST_IDEAS_PER_FUND)
                rows = best_idea.df_to_rows(
                    best_ideas_df,
                    provider_etf_id=etf_id,
                    value_date=target_date
                )
                best_idea.insert_bulk(rows)
                processed_etfs += 1

                successes.append((f"Generated {len(rows)} best ideas for ETF {etf_id} using holdings from {target_date.strftime('%Y-%m-%d')}."))

            except Exception as e:
                record_problem(etf_id=etf_id, error=str(e), message="Failed running best ideas for etfs", problem_etfs=problem_etfs)

        log.record_status(f"Finished batch run. Successful: {processed_etfs}/{total_etfs}")
        if successes:
            print("\n--- Summary of Successful Runs ---")
            print("\n".join(successes))
        else:
            print("\n--- No ETFs were successfully processed ---")

        print("\n")
        return total_etfs, processed_etfs, problem_etfs
    
    except Exception as e:
        log.record_error(f"Critical batch error: {e}")
        raise e