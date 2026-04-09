import log
from modules.object import batch_run, batch_run_log
from modules.object import provider, provider_etf
from modules.calc.best_ideas import find_best_ideas, to_holding_item, to_ticker_value_item, find_latest_common_date, filter_stale_holdings
from modules.object.provider_etf_holding import fetch_holding_dates_available_past_period, fetch_valid_holdings_by_provider_etf_id
from modules.object.ticker_value import fetch_ticker_dates_available_past_period, fetch_latest_price_date_for_ticker, fetch_tickers_by_symbols_on_date
from modules.object import best_idea

DAYS_NO_PRICING = 7
MIN_HOLDINGS_WITH_PRICES_PCT = 0.9
LOOK_BACK_FOR_COMMON_DATE = 14
MAX_BEST_IDEAS_PER_FUND = 10

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

        for p in providers:
            pe_list = provider_etf.fetch_by_provider_id(p.id)
            total_etfs += len(pe_list)
            log.record_status(f"Starting processing provider {p.name} with {len(pe_list)} ETFs")

            for pe in pe_list:
                try:
                    # 1. Find the latest date where both holdings and ticker values are available for this ETF
                    available_holding_dates = fetch_holding_dates_available_past_period(pe.id, LOOK_BACK_FOR_COMMON_DATE)
                    if not available_holding_dates:
                        record_problem(batch_run_id=batch_run_id, provider=p, etf=pe, error=f"No holdings have been downloaded for the past {LOOK_BACK_FOR_COMMON_DATE} days", message=None, problem_etfs=problem_etfs)
                        continue

                    available_ticker_dates = fetch_ticker_dates_available_past_period(pe.id, LOOK_BACK_FOR_COMMON_DATE)
                    if not available_ticker_dates:
                        record_problem(batch_run_id=batch_run_id, provider=p, etf=pe, error=f"No ticker values are available for the past {LOOK_BACK_FOR_COMMON_DATE} days", message=None, problem_etfs=problem_etfs)
                        continue

                    latest_common_date = find_latest_common_date(available_holding_dates, available_ticker_dates)
                    if latest_common_date is None:
                        message = f"Latest holdings date: {max(available_holding_dates).strftime('%b %d, %Y')}, Latest ticker value date: {max(available_ticker_dates).strftime('%b %d, %Y')}"
                        record_problem(batch_run_id=batch_run_id, provider=p, etf=pe, error="Data sources out of sync", message=message, problem_etfs=problem_etfs)
                        continue

                    # 2. Get holdings and remove stale tickers (no recent price)
                    raw_holdings = fetch_valid_holdings_by_provider_etf_id(pe.id, latest_common_date)
                    holding_items = [to_holding_item(h) for h in raw_holdings]

                    last_price_dates = {
                        h.ticker: fetch_latest_price_date_for_ticker(h.ticker, latest_common_date)
                        for h in holding_items if h.ticker
                    }
                    valid_holdings, stale_tickers = filter_stale_holdings(holding_items, last_price_dates, latest_common_date, DAYS_NO_PRICING)

                    for stale_ticker in stale_tickers:
                        record_problem(batch_run_id=batch_run_id, provider=p, etf=pe, error="STALE HOLDING", message=f"{stale_ticker} has no prices for over {DAYS_NO_PRICING} days", problem_etfs=problem_etfs)

                    if not valid_holdings:
                        record_problem(batch_run_id=batch_run_id, provider=p, etf=pe, error="No holdings with recent prices", message=None, problem_etfs=problem_etfs)
                        continue

                    # 3. Fetch prices and check coverage
                    ticker_symbols = [h.ticker for h in valid_holdings if h.ticker]
                    values = fetch_tickers_by_symbols_on_date(ticker_symbols, latest_common_date)
                    value_items = [to_ticker_value_item(v) for v in values]

                    coverage_ratio = len(value_items) / len(valid_holdings)
                    if coverage_ratio < MIN_HOLDINGS_WITH_PRICES_PCT:
                        missing_count = len(valid_holdings) - len(value_items)
                        msg = f"Coverage {coverage_ratio:.1%}. Missing {missing_count} prices for date {latest_common_date.strftime('%Y-%m-%d')}"
                        record_problem(batch_run_id=batch_run_id, provider=p, etf=pe, error="Insufficient pricing coverage", message=msg, problem_etfs=problem_etfs)
                        continue

                    # 4. Align holdings to only priced symbols before computing best ideas
                    priced_symbols = {v.symbol for v in value_items}
                    final_holdings = [h for h in valid_holdings if h.ticker in priced_symbols]

                    # 5. Generate and insert
                    best_ideas_df = find_best_ideas(final_holdings, value_items, MAX_BEST_IDEAS_PER_FUND)
                    rows = best_idea.df_to_rows(best_ideas_df, provider_etf_id=pe.id, value_date=latest_common_date)
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
