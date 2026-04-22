import log
from modules.object import batch_run
from modules.object.ticker import fetch_all_valid
from modules.ticker.resolver import populate_esg, get_full_symbol


def run() -> int:
    batch_run_id = batch_run.insert(batch_run.BatchRun(process='esg_update', activation='auto'))
    tickers = fetch_all_valid()
    log.record_status(f"ESG update: refreshing {len(tickers)} tickers.")

    count = 0
    for t in tickers:
        assert t.id is not None
        populate_esg(t.id, get_full_symbol(t))
        count += 1

    batch_run.update_completed_at(batch_run_id)
    log.record_status(f"ESG update complete: {count} tickers refreshed.")
    return count
