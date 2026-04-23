import log
from datetime import date, timedelta
from typing import List
from modules.object import batch_run
from modules.object import best_idea, fund, fund_holding, fund_holding_change, ticker, ticker_value
from modules.calc.model_fund import (
    FundChangesResult, results_to_string, to_fund_protocol, getStrategyFromJson,
    apply_equal_weights, apply_market_cap_weights,
)
from modules.calc import model_fund

MARKET_CAP_LOOKBACK_DAYS = 7


def run() -> List[FundChangesResult]:
    try:
        batch_run_id = batch_run.insert(batch_run.BatchRun(process="funds_update", activation="auto"))

        funds = [to_fund_protocol(f) for f in fund.fetch_all()]
        log.record_status(f"Running Fund Update batch job ID {batch_run_id} - will process {len(funds)} funds.")

        today = date.today()
        yesterday = today - timedelta(days=1)

        all_results: List[FundChangesResult] = []
        for f in funds:
            strategy = getStrategyFromJson(f.strategy)

            results = model_fund.generate(
                today=today,
                fund=f,
                previous_holdings=fund_holding.fetch_funds_holdings(f.id, yesterday),
                best_ideas_module=best_idea,
            )

            if results.holdings:
                if strategy.allocation == 'market_cap':
                    ticker_ids = [h.ticker_id for h in results.holdings]
                    mc_values = ticker_value.fetch_latest_market_caps_within_window(ticker_ids, today, MARKET_CAP_LOOKBACK_DAYS)
                    mc_map = {tv.ticker_id: tv.market_cap for tv in mc_values if tv.market_cap}
                    apply_market_cap_weights(results.holdings, mc_map)
                else:
                    apply_equal_weights(results.holdings)

            fund_holding.insert_fund_holding(results.holdings)
            fund_holding_change.insert_fund_changes(results.changes)

            log.record_status(results_to_string(results, ticker))
            all_results.append(results)

        batch_run.update_completed_at(batch_run_id)
        log.record_status("Finished Fund Update batch run.\n")
        return all_results

    except Exception as e:
        log.record_error(f"Error in fund update batch run: {e}")
        raise e
