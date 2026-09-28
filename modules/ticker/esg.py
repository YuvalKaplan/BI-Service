import log
from modules.core import api_stocks
from modules.calc import esg as esg_rules
from modules.object import batch_run
from modules.object.ticker import fetch_all_valid_companies, update_esg_data
from modules.ticker import resolver


def populate_esg(ticker_id: int, full_symbol: str) -> None:
    """Fetches the FMP ESG disclosure and risk rating of one ticker and stores them with its
    esg_qualified flag (modules/calc/esg.py). Called for every new ticker at registration."""
    try:
        disclosure, rating = api_stocks.fetch_esg_data(full_symbol)
        esg_qualified, esg_factors = esg_rules.qualify(disclosure, rating)
        update_esg_data(ticker_id, esg_qualified, esg_factors)
    except Exception as e:
        log.record_notice(f"Failed to store ESG for '{full_symbol}': {e}")


def refresh_all() -> int:
    """Refreshes the ESG data of every valid company (the weekly Sunday run)."""
    symbols = resolver.TickerResolver(resolver.TickerResolver.POPULATE_TICKER)
    batch_run_id = batch_run.insert(batch_run.BatchRun(process='esg_update', activation='auto'))
    # ESG is a company-level attribute: share-class siblings (master_ticker_id set) are skipped,
    # since downstream consumers (best ideas, fund composition) key off the master's row.
    tickers = fetch_all_valid_companies()
    log.record_status(f"ESG update: refreshing {len(tickers)} companies.")

    count = 0
    for t in tickers:
        populate_esg(t.id, symbols.get_full_symbol(t))
        count += 1

    batch_run.update_completed_at(batch_run_id)
    log.record_status(f"ESG update complete: {count} tickers refreshed.")
    return count
