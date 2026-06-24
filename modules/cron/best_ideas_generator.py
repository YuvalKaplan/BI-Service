import log
import pandas as pd
from modules.object import batch_run, batch_run_log
from modules.object import provider, provider_etf
from modules.object.provider_etf_holding import fetch_latest_holdings_for_etf
from modules.object.ticker_value import fetch_latest_market_caps_within_window
from modules.object import best_idea, benchmark

DAYS_NO_MARKET_CAP = 5
MIN_HOLDINGS_WITH_PRICES_PCT = 0.95
LOOK_BACK_WINDOW = 7
MAX_BEST_IDEAS_PER_FUND = 10
HOLDING_DELTA_LIMIT_DROP_OFF = 0.20


def _find_best_ideas(
    holdings: list,
    market_cap_values: list,
    limit: int | None = None,
    benchmark_weights: dict[int, float] | None = None,
) -> pd.DataFrame:
    df_holdings = pd.DataFrame(
        {"ticker_id": h.ticker_id, "market_value": h.market_value}
        for h in holdings
        if h.ticker_id and h.market_value and h.market_value > 0
    )

    df_values = pd.DataFrame(
        {"ticker_id": v.ticker_id, "market_cap": v.market_cap}
        for v in market_cap_values
        if v.ticker_id and v.market_cap
    )

    df = df_holdings.merge(df_values, on="ticker_id", how="inner")

    if df.empty:
        raise ValueError("No overlapping ticker_ids between holdings and market caps")

    total_etf_value = df["market_value"].sum()
    df["etf_weight"] = df["market_value"] / total_etf_value

    if benchmark_weights:
        # full_universe: each stock's weight in the external large-cap universe
        df["benchmark_weight"] = df["ticker_id"].map(benchmark_weights).fillna(0.0)
    else:
        # self: market-cap weight within the ETF's own holdings
        total_market_cap = df["market_cap"].sum()
        df["benchmark_weight"] = df["market_cap"] / total_market_cap

    df["delta"] = df["etf_weight"] - df["benchmark_weight"]

    best_ideas = df[df["delta"] > 0].sort_values("delta", ascending=False)

    dropped = best_ideas[best_ideas["delta"] > HOLDING_DELTA_LIMIT_DROP_OFF]
    if not dropped.empty:
        log.record_status(f"Dropping {len(dropped)} best-idea(s) above delta limit {HOLDING_DELTA_LIMIT_DROP_OFF}: {dropped['ticker_id'].tolist()}")
    best_ideas = best_ideas[best_ideas["delta"] <= HOLDING_DELTA_LIMIT_DROP_OFF]

    if limit is not None:
        if limit <= 0:
            raise ValueError("limit must be a positive integer")
        best_ideas = best_ideas.head(limit)

    return best_ideas.reset_index(drop=True)


def record_problem(batch_run_id: int, provider: provider.Provider, etf: provider_etf.ProviderEtf, error: str, message: str | None, problem_etfs: list[str]) -> None:
    item_info = f"[Provider: '{provider.name}' ({provider.id}), ETF: '{etf.name}' ({etf.id})]"
    record = f"{item_info}\t{error}\t{message or ''}"
    log.record_status(record)
    batch_run_log.insert(batch_run_log.BatchRunLog(batch_run_id=batch_run_id, note=record))
    problem_etfs.append(record)

def run() -> tuple[int, int, list[str]]:
    try:
        batch_run_id = None
        if batch_run_id is None:
            batch_run_id = batch_run.insert(batch_run.BatchRun(process='best_ideas_generator', activation='auto'))

        providers = provider.fetch_active_providers()
        log.record_status(f"Running Best Ideas Generator batch job ID {batch_run_id} - will proccess {len(providers)} providers.")

        total_etfs = 0
        generated_etfs = 0
        problem_etfs: list[str] = []

        # Build a cache of {benchmark_id: {ticker_id: weight}} for all ETFs that have one.
        # Fetched once per unique benchmark_id across all providers.
        _bm_cache: dict[int, dict[int, float]] = {}

        for p in providers:
            pe_list = provider_etf.fetch_by_provider_id(p.id)
            total_etfs += len(pe_list)
            log.record_status(f"Starting processing provider {p.name} with {len(pe_list)} ETFs")

            for pe in pe_list:
                try:
                    # 1. Fetch holdings for the latest holding date within the look-back window
                    raw_holdings = fetch_latest_holdings_for_etf(pe.id, LOOK_BACK_WINDOW)
                    if not raw_holdings:
                        record_problem(batch_run_id=batch_run_id, provider=p, etf=pe, error=f"No holdings have been downloaded for the past {LOOK_BACK_WINDOW} days", message=None, problem_etfs=problem_etfs)
                        continue

                    holding_date = raw_holdings[0].holding_date

                    # 2. Fetch latest market caps within +/- 5 days of the holding date.
                    # Going a few days after the holding is useful if we have a new ticker that was just added to the
                    # holdings as the first market_cap downloaded using the profile API may be a day or two after the holding date..
                    ticker_ids = [h.ticker_id for h in raw_holdings if h.ticker_id]
                    market_cap_values = fetch_latest_market_caps_within_window(ticker_ids, holding_date, DAYS_NO_MARKET_CAP)

                    # 3. Identify and report stale tickers (no market cap within window)
                    priced_ids = {v.ticker_id for v in market_cap_values}
                    for h in raw_holdings:
                        if h.ticker_id and h.ticker_id not in priced_ids:
                            record_problem(batch_run_id=batch_run_id, provider=p, etf=pe, error="STALE HOLDING", message=f"ticker_id={h.ticker_id} has no market cap for over {DAYS_NO_MARKET_CAP} days", problem_etfs=problem_etfs)

                    # 4. Coverage check
                    coverage_ratio = len(market_cap_values) / len(raw_holdings)
                    if coverage_ratio < MIN_HOLDINGS_WITH_PRICES_PCT:
                        missing_count = len(raw_holdings) - len(market_cap_values)
                        msg = f"Coverage {coverage_ratio:.1%}. Missing {missing_count} market caps for holding date {holding_date.strftime('%Y-%m-%d')}"
                        record_problem(batch_run_id=batch_run_id, provider=p, etf=pe, error="Insufficient market cap coverage", message=msg, problem_etfs=problem_etfs)
                        continue

                    final_holdings = [h for h in raw_holdings if h.ticker_id in priced_ids]

                    # 5a. Always generate self-benchmark best ideas
                    self_df = _find_best_ideas(final_holdings, market_cap_values, MAX_BEST_IDEAS_PER_FUND)
                    best_idea.insert_bulk(best_idea.df_to_rows(self_df, provider_etf_id=pe.id, value_date=holding_date, benchmark_mode='self'))

                    # 5b. If this ETF has a benchmark configured, also generate full_universe best ideas
                    if pe.benchmark_id:
                        if pe.benchmark_id not in _bm_cache:
                            bm_holdings = benchmark.fetch_latest_holdings(pe.benchmark_id, LOOK_BACK_WINDOW)
                            _bm_cache[pe.benchmark_id] = {h.ticker_id: h.weight for h in bm_holdings if h.ticker_id}
                        bm_weights = _bm_cache.get(pe.benchmark_id)
                        if bm_weights:
                            universe_df = _find_best_ideas(final_holdings, market_cap_values, MAX_BEST_IDEAS_PER_FUND, benchmark_weights=bm_weights)
                            best_idea.insert_bulk(best_idea.df_to_rows(universe_df, provider_etf_id=pe.id, value_date=holding_date, benchmark_mode='full_universe'))

                    generated_etfs += 1

                except Exception as e:
                    record_problem(batch_run_id=batch_run_id, provider=p, etf=pe, error=f"{e}", message=None, problem_etfs=problem_etfs)

            log.record_status(f"Completed processing provider {p.name}")

        batch_run.update_completed_at(batch_run_id)
        log.record_status(f"Finished Best Ideas Generator batch run on {total_etfs} etfs.\n")
        return total_etfs, generated_etfs, problem_etfs

    except Exception as e:
        log.record_error(f"Error in best ideas generator batch run: {e}")
        raise e
