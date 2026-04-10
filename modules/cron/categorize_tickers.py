import log
from datetime import date, datetime, timedelta
from modules.object import batch_run
from modules.object import provider, categorize_etf, categorize_etf_holding, ticker, categorize_ticker
from modules.parse.url import scrape_categorizer
from modules.parse.convert import load, get_tickers
from modules.calc import classification

STALE_DAYS_FOR_SCRAPE = 90

def run() -> int:
    try:
        batch_run_id = None
        if batch_run_id is None:
            batch_run_id = batch_run.insert(batch_run.BatchRun('categorize_tickers', 'auto'))

        last_update = categorize_ticker.fetch_last_update()
        last_update_date = last_update.date() if last_update else None
        is_stale = last_update_date is None or (date.today() - last_update_date) > timedelta(days=STALE_DAYS_FOR_SCRAPE)

        etfs = []
        num_classified = 0
        if is_stale:
            etfs = categorize_etf.fetch_all()
            log.record_status(f"Running Categorize Tickers batch job ID {batch_run_id} - will proccess {len(etfs)} categorization ETFs.")

            for etf in etfs:
                d = scrape_categorizer(etf)
                log.record_status(f"Processing '{etf.name}' ETF for categorization.")
                if etf.id and d.etf.file_format and d.etf.mapping and d.file_name:
                    map = provider.getMappingFromJson(d.etf.mapping)
                    full_rows = load(etf_name=d.etf.name, file_format=d.etf.file_format, mapping=map, file_name=d.file_name, raw_data=d.data)
                    symbols = get_tickers(full_rows=full_rows, mapping=map)
                    categorize_etf_holding.insert_holding(etf.id, date.today(), symbols)

            categorize_ticker.sync_categorize_ticker()
            ct_symbols = categorize_ticker.fetch_symbols()
            factor_updates = classification.update_factor_cache(ct_symbols)
            categorize_ticker.bulk_update(factor_updates)
        else:
            log.record_status(f"Skipping categorize ETF scrape — last update was {last_update_date} - performed every {STALE_DAYS_FOR_SCRAPE} days.")
        
        all_categorized = categorize_ticker.fetch_all()
        if all_categorized:
            categorized_tickers = [classification.to_categorize_ticker_item(t) for t in all_categorized]
            classifier = classification.get_classifier(categorized_tickers)
            num_classified = ticker.mark_style(classifier)
        else:
            raise Exception("Categorize_ticker data is empty - can not use classification model.")

        batch_run.update_completed_at(batch_run_id)
        log.record_status(f"Finished Categorize Tickers batch run on {len(etfs)} items.\n")
        log.record_status(f"Categorized {num_classified} tickers.\n")
        return num_classified

    except Exception as e:
        log.record_error(f"Error in categorize tickers batch run: {e}")
        raise e