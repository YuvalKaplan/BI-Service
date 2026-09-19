import log
import pandas as pd
from datetime import date, timedelta
from typing import List
from modules.object import batch_run
from modules.object import best_idea, fund, fund_holding, fund_holding_change
from modules.calc import model_fund


def build_shared_context(as_of_date: date) -> tuple[pd.DataFrame, dict]:
    """
    Builds the (all_best_ideas_df, mc_map) inputs shared by every fund for a given
    as_of_date. Computed once per run/date and passed into activate_fund() rather
    than recomputed per fund.
    """
    all_best_ideas_df = best_idea.fetch_all_as_df(as_of_date=as_of_date)
    all_best_ideas_df = model_fund.resolve_canonical_ticker_ids(all_best_ideas_df)

    canonical_rows = all_best_ideas_df[
        all_best_ideas_df['ticker_id'] == all_best_ideas_df['canonical_ticker_id']
    ]
    mc_map = (
        canonical_rows[['canonical_ticker_id', 'market_cap']]
        .dropna(subset=['market_cap'])
        .drop_duplicates(subset='canonical_ticker_id')
        .set_index('canonical_ticker_id')['market_cap']
        .to_dict()
    )

    return all_best_ideas_df, mc_map


def activate_fund(
    fund_id: int,
    as_of_date: date,
    all_best_ideas_df: pd.DataFrame,
    mc_map: dict,
) -> model_fund.FundChangesResult | None:
    """
    Updates a single fund's holdings for as_of_date and persists the result.
    Returns None if the fund's strategy.recalc_frequency_days hasn't elapsed
    since its last recalculation (the fund's holdings are left untouched).
    """
    f = fund.fetch_by_id(fund_id)
    if f is None:
        raise Exception(f"Fund not found: fund_id={fund_id}")

    fund_protocol = model_fund.to_fund_protocol(f)
    previous_eval_date = as_of_date - timedelta(days=1)
    strategy = model_fund.getStrategyFromJson(fund_protocol.strategy)

    previous_holdings = fund_holding.fetch_funds_holdings(fund_id, previous_eval_date)

    if previous_holdings:
        days_since_recalc = (as_of_date - previous_holdings[0].holding_date).days
        if days_since_recalc < strategy.recalc_frequency_days:
            log.record_status(
                f"[funds_update] Skipping '{f.name}' — {days_since_recalc}d since last recalculation, "
                f"frequency is {strategy.recalc_frequency_days}d."
            )
            return None

    fund_ideas_df = all_best_ideas_df[
        all_best_ideas_df['benchmark_mode'] == strategy.benchmark
    ]

    results = model_fund.generate(
        today=as_of_date,
        fund=fund_protocol,
        previous_holdings=previous_holdings,
        all_best_ideas_df=fund_ideas_df,
        mc_map=mc_map,
    )

    fund_holding.insert_fund_holding(results.holdings)
    fund_holding_change.insert_fund_changes(results.changes)

    log.record_status(model_fund.results_to_string(results))
    return results


def run() -> List[model_fund.FundChangesResult]:
    try:
        batch_run_id = batch_run.insert(batch_run.BatchRun(process="funds_update", activation="auto"))

        funds = fund.fetch_all()
        log.record_status(f"Running Fund Update batch job ID {batch_run_id} - will process {len(funds)} funds.")

        today = date.today()
        all_best_ideas_df, mc_map = build_shared_context(today)

        all_results = [
            r for r in (activate_fund(f.id, today, all_best_ideas_df, mc_map) for f in funds)
            if r is not None
        ]

        batch_run.update_completed_at(batch_run_id)
        log.record_status("Finished Fund Update batch run.\n")
        return all_results

    except Exception as e:
        log.record_error(f"Error in fund update batch run: {e}")
        raise e
