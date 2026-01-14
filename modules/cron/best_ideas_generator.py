import log
import os
from datetime import date
from modules.object import batch_run, batch_run_log
from modules.object import provider, provider_etf
from modules.calc.best_ideas import find_best_ideas
from modules.object.provider_etf_holding import fetch_latest_by_provider_etf_id
from modules.object.ticker_value import fetch_latest_tickers_by_symbols
from modules.object import best_idea

MIN_HOLDINGS = 10
MAX_DATE_DIFF_DAYS = 5
MAX_BEST_IDEAS_PER_FUND = 15

def record_problem(batch_run_id: int, provider: provider.Provider, etf: provider_etf.ProviderEtf, error: str, message: str | None, problem_etfs: list[str]):
    item_info = f"[Provider: '{provider.name}' ({provider.id}), ETF: '{etf.name}' ({etf.id})]"
    record = "{:<100}{}".format(item_info, f"{error} {message or ''}")

    log.record_status(record)
    batch_run_log.insert(batch_run_log.BatchRunLog(batch_run_id=batch_run_id, note=record))
    problem_etfs.append(record)

def run() -> tuple[int, int, list[str]]:
    value_date = date.today()
    try:
        batch_run_id = None
        if batch_run_id is None:
            batch_run_id = batch_run.insert(batch_run.BatchRun('best_ideas_generator', 'auto'))

        providers = provider.fetch_active_providers()

        log.record_status(f"Running Best Ideas Generator batch job ID {batch_run_id} - will proccess {len(providers)} providers.")

        total_etfs = 0
        generated_etfs = 0
        problem_etfs: list[str] = []
        for p in providers:
            pe_list = provider_etf.fetch_by_provider_id(p.id)
            total_etfs += len(pe_list)
            log.record_status(f"Starting processing provider {p.name} with {len(pe_list)} ETFs")

            for pe in pe_list:
                try:
                    holdings = fetch_latest_by_provider_etf_id(pe.id)
                    if len(holdings) < MIN_HOLDINGS:
                        if len(holdings) == 0:
                            error = f"No holdings"
                        else:
                            error = f"Less than {MIN_HOLDINGS} holdings"
                        record_problem(batch_run_id=batch_run_id, provider=p, etf=pe, error=error, message=None, problem_etfs=problem_etfs)
                        continue

                    holdings_date = max(h.trade_date.date() for h in holdings)
                    tickers = [h.ticker for h in holdings]
                    values = fetch_latest_tickers_by_symbols(tickers)
                    stock_values_date = min(v.value_date for v in values if v.value_date is not None)

                    diff_days = abs((holdings_date - stock_values_date).days)
                    if diff_days > MAX_DATE_DIFF_DAYS:
                        message = f"holdings_date={holdings_date}, stock_values_date={stock_values_date}, diff={diff_days} days"
                        record_problem(batch_run_id=batch_run_id, provider=p, etf=pe, error="Data sources out of sync", message=message, problem_etfs=problem_etfs)
                        continue

                    best_ideas_df = find_best_ideas(holdings, values, MAX_BEST_IDEAS_PER_FUND)
                    rows = best_idea.df_to_rows(
                        best_ideas_df,
                        provider_etf_id=pe.id,
                        value_date=min(holdings_date, stock_values_date)
                    )
                    best_idea.insert_bulk(rows)
                    generated_etfs += 1
                except Exception as e:
                    record_problem(batch_run_id=batch_run_id, provider=p, etf=pe, error="Error in generating best ideas", message=None, problem_etfs=problem_etfs)

            log.record_status(f"Completed processing provider {p.name}")

        batch_run.update_completed_at(batch_run_id)       
        log.record_status(f"Finished Best Ideas Generator batch run on {total_etfs} etfs.\n")
        return total_etfs, generated_etfs, problem_etfs
    
    except Exception as e:
        log.record_error(f"Error in best ideas generator batch run: {e}")
        raise e