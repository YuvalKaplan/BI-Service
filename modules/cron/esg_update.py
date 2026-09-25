import log
from modules.object import batch_run
from modules.object.ticker import fetch_all_valid_companies
from modules.ticker.resolver import populate_esg, TickerResolver


def run() -> int:
    resolver = TickerResolver(TickerResolver.POPULATE_TICKER)
    batch_run_id = batch_run.insert(batch_run.BatchRun(process='esg_update', activation='auto'))
    # ESG is a company-level attribute: share-class siblings (master_ticker_id set) are skipped,
    # since downstream consumers (best ideas, fund composition) key off the master's row.
    tickers = fetch_all_valid_companies()
    log.record_status(f"ESG update: refreshing {len(tickers)} companies.")

    count = 0
    for t in tickers:
        assert t.id is not None
        populate_esg(t.id, resolver.get_full_symbol(t))
        count += 1

    batch_run.update_completed_at(batch_run_id)
    log.record_status(f"ESG update complete: {count} tickers refreshed.")
    return count
