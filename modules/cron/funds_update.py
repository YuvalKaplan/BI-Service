import log
from datetime import date
from typing import List
from dataclasses import dataclass
from modules.object import batch_run, batch_run_log
from modules.object.best_idea import fetch_best_ideas_by_ranking
from modules.object.fund import Fund, fetch_all
from modules.object.fund_holding import FundHolding, fetch_funds_holdings, insert_fund_holding
from modules.object.fund_holding_change import FundHoldingChange, insert_fund_changes
from modules.object.ticker import fetch_by_symbols


HOLDINGS_IN_FUND = 40
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

def run() -> List[FundChangesResult]:
    try:
        batch_run_id = None
        if batch_run_id is None:
            batch_run_id = batch_run.insert(batch_run.BatchRun('funds_update', 'auto'))

        funds = fetch_all()

        log.record_status(f"Running Fund Update batch job ID {batch_run_id} - will proccess {len(funds)} funds.")
        
        today = date.today()
        
        results: List[FundChangesResult] = []
        for f in funds:
            holdings_changed: list[FundHoldingChange] = [] 
            todays_holdings: List[FundHolding] = []
            fresh_ideas = fetch_best_ideas_by_ranking(ranking_level=5, style_type=f.style_type, cap_type=f.cap_type)
            yesterday_holdings = fetch_funds_holdings(f.id)
            
            # Find if we need to replace holdings - have droped 2 rankings
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
       
        batch_run.update_completed_at(batch_run_id)       
        log.record_status(f"Finished Fund Update batch run.\n")

        return results
    
    except Exception as e:
        log.record_error(f"Error in best ideas generator batch run: {e}")
        raise e