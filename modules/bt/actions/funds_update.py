import log
from datetime import date, timedelta
from typing import List
from modules.bt.object import best_idea, fund, fund_holding, fund_holding_change, ticker, ticker_value
from modules.object import provider_etf
from modules.bt.calc.model_fund import (
    FundChangesResult, results_to_string, to_fund_protocol, getStrategyFromJson,
    apply_equal_weights, apply_market_cap_weights,
)
from modules.bt.calc import model_fund

MARKET_CAP_LOOKBACK_DAYS = 7


def run(today: date) -> List[FundChangesResult]:
    try:
        funds = [to_fund_protocol(f) for f in fund.fetch_all()]
        log.record_status(f"Running Fund Update - will process {len(funds)} funds.")

        yesterday = today - timedelta(days=1)

        all_results: List[FundChangesResult] = []
        for f in funds:
            strategy = getStrategyFromJson(f.strategy)

            provider_etf_regions = None
            region_split = strategy.region.split if strategy.region is not None else None
            if region_split is not None and region_split.US is not None and region_split.International is not None and strategy.provider_etfs:
                provider_etf_regions = provider_etf.fetch_regions_by_ids(strategy.provider_etfs)

            results = model_fund.generate(
                today=today,
                fund=f,
                previous_holdings=fund_holding.fetch_funds_holdings(f.id, yesterday),
                best_ideas_module=best_idea,
                provider_etf_regions=provider_etf_regions,
            )

            if results.holdings:
                if strategy.allocation == 'market_cap':
                    symbols = [h.symbol for h in results.holdings]
                    mc_values = ticker_value.fetch_latest_market_caps_within_window(symbols, today, MARKET_CAP_LOOKBACK_DAYS)
                    mc_map = {tv.symbol: tv.market_cap for tv in mc_values if tv.market_cap}
                    apply_market_cap_weights(results.holdings, mc_map)
                else:
                    apply_equal_weights(results.holdings)

            fund_holding.insert_fund_holding(results.holdings)
            fund_holding_change.insert_fund_changes(results.changes)

            log.record_status(results_to_string(results, ticker))
            all_results.append(results)

        log.record_status("Finished Fund Update run.\n")
        return all_results

    except Exception as e:
        log.record_error(f"Error in fund update run: {e}")
        raise e
