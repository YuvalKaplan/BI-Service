import log
from modules.object import batch_run
from modules.object import provider, categorize_etf, ticker, categorize_ticker
from modules.parse.url import scrape_categorizer
from modules.parse.convert import load, get_tickers
from modules.calc import classification
from modules.calc.classification import to_categorize_ticker_item
def run() -> int:
    try:
        batch_run_id = None
        if batch_run_id is None:
            batch_run_id = batch_run.insert(batch_run.BatchRun('categorize_tickers', 'auto'))

        etfs = categorize_etf.fetch_all()
        
        log.record_status(f"Running Categorize Tickers batch job ID {batch_run_id} - will proccess {len(etfs)} items.")

        total_symbols = 0
        for etf in etfs:
            d = scrape_categorizer(etf)
            log.record_status(f"Processing '{etf.name}' ETF for categorization.")
            if etf.id and d.etf.file_format and d.etf.mapping and d.file_name:
                map = provider.getMappingFromJson(d.etf.mapping)
                full_rows = load(etf_name=d.etf.name, file_format=d.etf.file_format, mapping=map, file_name=d.file_name, raw_data=d.data)
                symbols = get_tickers(full_rows=full_rows, mapping=map)
                
                if etf.cap_type and etf.style_type:
                    total_symbols += len(symbols)
                    ticker.upsert_tickers_style_and_cap(symbols=symbols, cap_type=etf.cap_type, style_type=etf.style_type)
                
                categorize_etf.update_last_download(etf.id)

        categorize_ticker.sync_categorize_ticker()
        ct_symbols = categorize_ticker.fetch_symbols()
        factor_updates = classification.update_factor_cache(ct_symbols)
        categorize_ticker.bulk_update(factor_updates)
        categorized_tickers = [to_categorize_ticker_item(t) for t in categorize_ticker.fetch_all()]
        classifier = classification.get_classifier(categorized_tickers)
        ticker.mark_style(classifier)

        batch_run.update_completed_at(batch_run_id)
        log.record_status(f"Finished Categorize Tickers batch run on {len(etfs)} items.\n")
        log.record_status(f"Processed {total_symbols} tickers.\n")
        return total_symbols

    except Exception as e:
        log.record_error(f"Error in categorize tickers batch run: {e}")
        raise e