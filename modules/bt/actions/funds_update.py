import log
from datetime import date
from typing import List
from dataclasses import dataclass
from modules.bt.object.best_idea import fetch_best_ideas_by_ranking
from modules.bt.object.fund import Fund, getStrategyFromJson, fetch_all
from modules.bt.object.fund_holding import FundHolding, fetch_funds_holdings, insert_fund_holding
from modules.bt.object.fund_holding_change import FundHoldingChange, insert_fund_changes
from modules.bt.object.ticker import fetch_by_symbols


HOLDINGS_IN_FUND = 40
RANKING_LEVEL = 3
GAP_TO_SELL = 2

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
        funds = fetch_all()

        log.record_status(f"Running Fund Update - will proccess {len(funds)} funds.")
               
        results: List[FundChangesResult] = []
        for f in funds:
            strategy = getStrategyFromJson(f.strategy)
            provider_etfs = strategy.provider_etfs or []
        
            if (strategy.style.name == "blend") and (strategy.style.value is not None) and (strategy.style.growth is not None):
                fresh_ideas_value = fetch_best_ideas_by_ranking(ranking_level=RANKING_LEVEL, style_type="value", cap_type=strategy.cap.name, as_of_date=today, provider_etf_ids=provider_etfs)
                fresh_ideas_growth = fetch_best_ideas_by_ranking(ranking_level=RANKING_LEVEL, style_type="growth", cap_type=strategy.cap.name, as_of_date=today, provider_etf_ids=provider_etfs)
                growth_count = min(len(fresh_ideas_growth), round(strategy.holdings * strategy.style.growth/100))
                value_count = min(len(fresh_ideas_value), strategy.holdings - growth_count)
                fresh_ideas = (fresh_ideas_growth[:growth_count] + fresh_ideas_value[:value_count])
            else:
                fresh_ideas = fetch_best_ideas_by_ranking(ranking_level=RANKING_LEVEL, style_type=strategy.style.name, cap_type=strategy.cap.name, as_of_date=today, provider_etf_ids=provider_etfs)
            
            yesterday_holdings = fetch_funds_holdings(f.id, today)
            
            # Find if we need to replace holdings - have droped 2 rankings
            holdings_changed: list[FundHoldingChange] = [] 
            todays_holdings: List[FundHolding] = []
            
            for yh in yesterday_holdings:
                latest_state = next((x for x in fresh_ideas if x.symbol == yh.symbol), None)
                if latest_state is None or latest_state.ranking - yh.ranking >= GAP_TO_SELL:
                    # Should sell the holding
                    holdings_changed.append(FundHoldingChange(fund_id=f.id, symbol=yh.symbol, change_date=today, direction='sell'))
                else:
                    # Keep the holding
                    yh.holding_date = today
                    todays_holdings.append(yh)

            # If new holdings are needed get then from the fresh - as long as they are not already in the fund.
            missing = HOLDINGS_IN_FUND - len(todays_holdings) 
            if missing != 0:
                existing_symbols = {th.symbol for th in todays_holdings}
                for fi in fresh_ideas:
                    if fi.symbol in existing_symbols:
                        continue

                    # add holding
                    todays_holdings.append(FundHolding(fund_id=f.id, holding_date=today, symbol=fi.symbol, ranking=fi.ranking))
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