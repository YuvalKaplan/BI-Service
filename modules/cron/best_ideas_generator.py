import log
import os
from datetime import date
from modules.object import batch_run, batch_run_log
from modules.object import provider, provider_etf
from modules.calc.best_ideas import find_best_ideas
from modules.object.provider_etf_holding import fetch_holding_dates_available_past_week, fetch_holdings_by_provider_etf_id
from modules.object.ticker_value import fetch_price_dates_available_past_week, fetch_tickers_by_symbols_on_date
from modules.object import best_idea

MIN_HOLDINGS = 10
MAX_DATE_DIFF_DAYS = 5
MAX_BEST_IDEAS_PER_FUND = 15

def record_problem(batch_run_id: int, provider: provider.Provider, etf: provider_etf.ProviderEtf, error: str, message: str | None, problem_etfs: list[str]):
    item_info = f"[Provider: '{provider.name}' ({provider.id}), ETF: '{etf.name}' ({etf.id})]"
    record = f"{item_info}\t{error}\t{message or ''}"

    log.record_status(record)
    batch_run_log.insert(batch_run_log.BatchRunLog(batch_run_id=batch_run_id, note=record))
    problem_etfs.append(record)

def run() -> tuple[int, int, list[str]]:
    try:
        batch_run_id = None
        if batch_run_id is None:
            batch_run_id = batch_run.insert(batch_run.BatchRun('best_ideas_generator', 'auto'))

        providers = provider.fetch_active_providers()

        log.record_status(f"Running Best Ideas Generator batch job ID {batch_run_id} - will proccess {len(providers)} providers.")

        total_etfs = 0
        generated_etfs = 0
        problem_etfs: list[str] = []
        availabe_price_dates = fetch_price_dates_available_past_week()
        for p in providers:
            pe_list = provider_etf.fetch_by_provider_id(p.id)
            total_etfs += len(pe_list)
            log.record_status(f"Starting processing provider {p.name} with {len(pe_list)} ETFs")

            for pe in pe_list:
                try:
                    available_holding_dates = fetch_holding_dates_available_past_week(pe.id)
                    latest_common_date = max(set(availabe_price_dates) & set(available_holding_dates), default=None)
                    if not latest_common_date:
                        message = f"Latest holdings date: {max(available_holding_dates).strftime("%b %d, %Y")}, Latest price date: {max(availabe_price_dates).strftime("%b %d, %Y")}"
                        record_problem(batch_run_id=batch_run_id, provider=p, etf=pe, error="Data sources out of sync", message=message, problem_etfs=problem_etfs)
                        continue

                    holdings = fetch_holdings_by_provider_etf_id(pe.id, latest_common_date)
                    if len(holdings) < MIN_HOLDINGS:
                        if len(holdings) == 0:
                            error = f"No holdings"
                        else:
                            error = f"Less than {MIN_HOLDINGS} holdings"
                        record_problem(batch_run_id=batch_run_id, provider=p, etf=pe, error=error, message=None, problem_etfs=problem_etfs)
                        continue
                    
                    tickers = [h.ticker for h in holdings]
                    values = fetch_tickers_by_symbols_on_date(tickers, latest_common_date)

                    if len(values) < int(0.9 * len(holdings)):
                        record_problem(batch_run_id=batch_run_id, provider=p, etf=pe, error="Too many prices missing (less than 90 percent of holdngs)", message=None, problem_etfs=problem_etfs)
                        continue

                    best_ideas_df = find_best_ideas(holdings, values, MAX_BEST_IDEAS_PER_FUND)
                    rows = best_idea.df_to_rows(
                        best_ideas_df,
                        provider_etf_id=pe.id,
                        value_date=latest_common_date
                    )
                    best_idea.insert_bulk(rows)
                    generated_etfs += 1
                except Exception as e:
                    record_problem(batch_run_id=batch_run_id, provider=p, etf=pe, error=f"{e}", message=None, problem_etfs=problem_etfs)

            log.record_status(f"Completed processing provider {p.name}")

        batch_run.update_completed_at(batch_run_id)       
        log.record_status(f"Finished Best Ideas Generator batch run on {total_etfs} etfs.\n")
        return total_etfs, generated_etfs, problem_etfs
    
    except Exception as e:
        log.record_error(f"Error in best ideas generator batch run: {e}")
        raise e