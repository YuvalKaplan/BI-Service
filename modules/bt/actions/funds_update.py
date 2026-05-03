import log
from datetime import date, timedelta
from typing import List
from modules.bt.object import best_idea, fund, fund_holding, fund_holding_change
from modules.bt.calc import model_fund


def run(today: date) -> List[model_fund.FundChangesResult]:
    try:
        funds = [model_fund.to_fund_protocol(f) for f in fund.fetch_all()]
        log.record_status(f"Running Fund Update - will process {len(funds)} funds.")

        yesterday = today - timedelta(days=1)

        max_ranking = model_fund.USE_RANKING_LOW + max(5, 2)
        all_best_ideas_df = best_idea.fetch_all_as_df(as_of_date=today, ranking_level=max_ranking)
        log.record_status(
            f"Best ideas loaded for fund generation ({len(all_best_ideas_df)} rows):\n"
            + all_best_ideas_df.to_string(index=False) + "\n"
        )

        mc_map = (
            all_best_ideas_df[['symbol', 'market_cap']]
            .dropna(subset=['market_cap'])
            .drop_duplicates(subset='symbol')
            .set_index('symbol')['market_cap']
            .to_dict()
        )

        all_results: List[model_fund.FundChangesResult] = []
        for f in funds:
            results = model_fund.generate(
                today=today,
                fund=f,
                previous_holdings=fund_holding.fetch_funds_holdings(f.id, yesterday),
                all_best_ideas_df=all_best_ideas_df,
                mc_map=mc_map,
            )

            fund_holding.insert_fund_holding(results.holdings)
            fund_holding_change.insert_fund_changes(results.changes)

            log.record_status(model_fund.results_to_string(results))
            all_results.append(results)

        log.record_status("Finished Fund Update run.\n")
        return all_results

    except Exception as e:
        log.record_error(f"Error in fund update run: {e}")
        raise e
