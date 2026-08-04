import log
from datetime import date, timedelta
from modules.core.db import ENVIRONMENT
from modules.object import provider_etf_holding, fund_holding, fund_holding_change
from modules.cron import best_ideas_generator, funds_update

DEFAULT_INCEPTION_DATE = date(2026, 1, 15)


def run(fund_id: int, inception_date: date = DEFAULT_INCEPTION_DATE) -> None:
    if ENVIRONMENT == 'production':
        raise RuntimeError("Refusing to run fund simulation against production — it erases the target fund's holdings history.")

    end_date = provider_etf_holding.fetch_max_holding_date()
    if end_date is None:
        raise RuntimeError("No provider_etf_holding data found — nothing to simulate against.")
    if end_date < inception_date:
        raise RuntimeError(f"Latest available holding date ({end_date}) is before inception_date ({inception_date}).")

    log.record_status(f"[sim] Erasing prior fund_holding/fund_holding_change for fund_id={fund_id}")
    fund_holding.delete_all_for_fund(fund_id)
    fund_holding_change.delete_all_for_fund(fund_id)

    log.record_status(f"[sim] Running fund_id={fund_id} simulation from {inception_date} to {end_date}")

    current = inception_date
    while current <= end_date:
        if current.weekday() == 2:  # Wednesday
            best_ideas_generator.run(as_of_date=current)
            all_best_ideas_df, mc_map = funds_update.build_shared_context(current)
            funds_update.activate_fund(fund_id, current, all_best_ideas_df, mc_map)
        current += timedelta(days=1)

    log.record_status(f"[sim] Finished fund_id={fund_id} simulation from {inception_date} to {end_date}")
