import log
from datetime import date, timedelta
from typing import List
from modules.object import batch_run
from modules.object import best_idea, fund, fund_holding, fund_holding_change, ticker
from modules.calc.model_fund import FundChangesResult, results_to_string, to_fund_protocol
from modules.calc import model_fund


def run() -> List[FundChangesResult]:
    try:
        batch_run_id = batch_run.insert(batch_run.BatchRun("funds_update", "auto"))

        funds = [to_fund_protocol(f) for f in fund.fetch_all()]
        log.record_status(f"Running Fund Update batch job ID {batch_run_id} - will process {len(funds)} funds.")

        today = date.today()
        yesterday = today - timedelta(days=1)

        all_results: List[FundChangesResult] = []
        for f in funds:
            results = model_fund.generate(
                today=today,
                fund=f,
                previous_holdings=fund_holding.fetch_funds_holdings(f.id, yesterday),
                best_ideas_module=best_idea,
            )

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
