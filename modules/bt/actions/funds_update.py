import log
from datetime import date, timedelta
from typing import List
from dataclasses import dataclass
from modules.bt.object.best_idea import fetch_best_ideas_by_ranking, average_best_ideas_delta_for_etf
from modules.bt.object.fund import Fund, getStrategyFromJson, fetch_all
from modules.bt.object.fund_holding import FundHolding, fetch_funds_holdings, insert_fund_holding
from modules.bt.object.fund_holding_change import FundHoldingChange, insert_fund_changes
from modules.bt.object.ticker import fetch_by_symbols

USE_RANKING_HIGH = 1
USE_RANKING_LOW = 1
RANKING_GAP_DROP_FROM_ENTRY = 2
FETCH_RANKING_LEVEL = USE_RANKING_LOW + RANKING_GAP_DROP_FROM_ENTRY 

@dataclass
class FundChangesResult:
    fund: Fund
    changes: List[FundHoldingChange]

def results_to_string(results: List[FundChangesResult]):
    # Print out
    aggregator = ""
    all_symbols: set[str] = {
        ch.symbol
        for r in results
        for ch in r.changes
    }
    tickers = fetch_by_symbols(list(all_symbols))
    ticker_by_symbol = {t.symbol: t for t in tickers}

    for r in results:
        aggregator += (f"{r.fund.name}\n" + "=" * 20 + "\n")
        if len(r.changes) == 0:
            aggregator += "No changes\n\n\n"
        else:
            aggregator += "{:<12}{:<15}{:<12}{:<15}{:<10}{}\n".format('Direction', 'Date', 'Ranking', 'Appearances', 'Symbol', 'Name')
            for ch in r.changes:
                ticker = ticker_by_symbol.get(ch.symbol)
                date_str = ch.change_date.strftime("%Y-%m-%d") if ch.change_date else "---"
                ranking = ch.ranking if ch.ranking else "---"
                appearances = ch.appearances if ch.appearances else "---"
                name_str = ticker.name if ticker else "---"
                aggregator += "{:<12}{:<15}{:<12}{:<15}{:<10}{}\n".format(ch.direction, date_str, ranking, appearances, ch.symbol, name_str)

            aggregator += ("-" * 30 + "\n\n")

    return aggregator

def run(today: date) -> List[FundChangesResult]:
    try:
        # if today == date(2025, 1, 13):
        #     print('stop!!!')  

        funds = fetch_all()

        log.record_status(f"Running Fund Update - will proccess {len(funds)} funds.")
               
        results: List[FundChangesResult] = []
        for f in funds:
            strategy = getStrategyFromJson(f.strategy)
            provider_etfs = strategy.provider_etfs or []
            
            if (strategy.style.name == "blend") and (strategy.style.value is not None) and (strategy.style.growth is not None):
                fetched_growth = fetch_best_ideas_by_ranking(ranking_level=FETCH_RANKING_LEVEL, style_type="growth", cap_type=strategy.cap.name, as_of_date=today, provider_etf_ids=provider_etfs)
                fetched_value = fetch_best_ideas_by_ranking(ranking_level=FETCH_RANKING_LEVEL, style_type="value", cap_type=strategy.cap.name, as_of_date=today, provider_etf_ids=provider_etfs)
                
                if USE_RANKING_HIGH != 1:
                    fetched_growth = [item for item in fetched_growth if USE_RANKING_HIGH <= item.ranking]
                    fetched_value = [item for item in fetched_value if USE_RANKING_HIGH <= item.ranking]
                
                fetched = fetched_growth + fetched_value

                # Filter for items with ranking between not over USE_RANKING_LOW
                growth_in_range = [item for item in fetched_growth if item.ranking <= USE_RANKING_LOW]
                value_in_range = [item for item in fetched_value if item.ranking <= USE_RANKING_LOW]
                in_range = growth_in_range + value_in_range
                
                growth_count = min(len(growth_in_range), round(strategy.holdings * strategy.style.growth/100))
                value_count = min(len(value_in_range), strategy.holdings - growth_count)
                ideal_holdings = (growth_in_range[:growth_count] + value_in_range[:value_count])
            else:
                fetched = fetch_best_ideas_by_ranking(ranking_level=FETCH_RANKING_LEVEL, style_type=strategy.style.name, cap_type=strategy.cap.name, as_of_date=today, provider_etf_ids=provider_etfs)
                
                if USE_RANKING_HIGH != 1:
                    fetched = [item for item in fetched if USE_RANKING_HIGH <= item.ranking]

                # Filter for items with ranking between not over USE_RANKING_LOW
                in_range = [item for item in fetched if item.ranking <= USE_RANKING_LOW]

                ideal_holdings = in_range[:strategy.holdings]

            previous_holdings = fetch_funds_holdings(f.id, today - timedelta(days=1))
            
            # Find if we need to replace holdings - have droped 2 rankings
            holdings_changed: list[FundHoldingChange] = [] 
            todays_holdings: List[FundHolding] = []

            if len(ideal_holdings) == 0:
                # Carry over everything from yesterday to today
                for ph in previous_holdings:
                    ph.holding_date = today
                    todays_holdings.append(ph)
                log.record_status(f"Ideal holdings empty (prices not available relative to holding date). Carried over {len(todays_holdings)} holdings.")
            else:
                for ph in previous_holdings:
                    found_in_ideal = next((x for x in ideal_holdings if x.symbol == ph.symbol), None)
                    # if found_in_ideal is None or found_in_ideal.ranking - ph.ranking >= GAP_TO_SELL:
                    if found_in_ideal is None:
                        found_in_fetched = next((x for x in fetched if x.symbol == ph.symbol), None)
                        if found_in_fetched is None:
                            # Sell: the holding is no longer in the top MIN_RANK_DROP
                            holdings_changed.append(
                                FundHoldingChange(
                                    fund_id=f.id, 
                                    symbol=ph.symbol, 
                                    change_date=today, 
                                    direction='sell',
                                    reason='Not in best ideas top levels'
                                ))
                            continue

                        if found_in_fetched.ranking - ph.ranking >= RANKING_GAP_DROP_FROM_ENTRY:
                            holdings_changed.append(
                                FundHoldingChange(
                                    fund_id=f.id, 
                                    symbol=ph.symbol, 
                                    change_date=today, 
                                    direction='sell',
                                    reason='Dropped below min ranking'
                                ))
                            continue
                        
                        # FACTOR_OF_AVERAGE_DELTA = 0.75
                        # RANKING_LEVEL_FOR_AVERAGE_DELTA = 5
                        # if found_in_fetched.source_etf_id == ph.source_etf_id:
                        #     average_delta = average_best_ideas_delta_for_etf(provider_etf_id=found_in_fetched.source_etf_id, as_of_date=today, use_rankings=RANKING_LEVEL_FOR_AVERAGE_DELTA)
                        #     if found_in_fetched.max_delta < FACTOR_OF_AVERAGE_DELTA * average_delta:
                        #         # Sell: the delta is under the average delta by a factor of FACTOR_OF_AVERAGE_DELTA
                        #         holdings_changed.append(
                        #             FundHoldingChange(
                        #                 fund_id=f.id, 
                        #                 symbol=ph.symbol, 
                        #                 change_date=today, 
                        #                 direction='sell', 
                        #                 reason='Delta gap', 
                        #                 ranking=found_in_fetched.ranking, 
                        #                 appearances=found_in_fetched.appearances, 
                        #                 max_delta=found_in_fetched.max_delta, 
                        #                 top_delta_provider_etf_id=found_in_fetched.source_etf_id, 
                        #                 all_provider_etf_ids=found_in_fetched.all_provider_ids
                        #             ))
                        #         continue
                    
                    # Keep the holding
                    ph.holding_date = today
                    todays_holdings.append(ph)

                # If new holdings are needed get then from the fresh - as long as they are not already in the fund.
                missing = strategy.holdings - len(todays_holdings) 
                if missing != 0:
                    existing_symbols = {th.symbol for th in todays_holdings}
                    for fi in ideal_holdings:
                        if fi.symbol in existing_symbols:
                            continue

                        # add holding
                        todays_holdings.append(FundHolding(fund_id=f.id, holding_date=today, symbol=fi.symbol, ranking=fi.ranking, source_etf_id=fi.source_etf_id, max_delta=fi.max_delta))
                        holdings_changed.append(FundHoldingChange(fund_id=f.id, symbol=fi.symbol, change_date=today, direction="buy", ranking=fi.ranking, appearances=fi.appearances, max_delta=fi.max_delta, top_delta_provider_etf_id=fi.source_etf_id, all_provider_etf_ids=fi.all_provider_ids))

                        existing_symbols.add(fi.symbol)
                        missing -= 1

                        if missing == 0:
                            break

            # Save todays holdings and changes
            insert_fund_holding(todays_holdings)
            insert_fund_changes(holdings_changed)

            results.append(FundChangesResult(fund=f, changes=holdings_changed))
            log.record_status(f"Completed processing fund {f.name}")
       
        log.record_status(f"{results_to_string(results)}")
        log.record_status(f"Finished Fund Update run.\n")

        return results
    
    except Exception as e:
        log.record_error(f"Error in best ideas generator run: {e}")
        raise e