import log
from modules.object import batch_run
from modules.object import provider, categorize_etf, ticker
from modules.parse.url import scrape_categorizer
from modules.parse.convert import load, get_tickers

def run() -> int:
    try:
        batch_run_id = None
        if batch_run_id is None:
            batch_run_id = batch_run.insert(batch_run.BatchRun('categorize_tickers', 'auto'))

        ticker.sync_tickers_with_etf_holdings()

        etfs = categorize_etf.fetch_all()
        
        log.record_status(f"Running categorize tickers batch job ID {batch_run_id} - will proccess {len(etfs)} items.")

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

        batch_run.update_completed_at(batch_run_id)       
        log.record_status(f"Finished categorize tickers batch run on {len(etfs)} items.\n")
        log.record_status(f"Processed {total_symbols} tickers.\n")
        return total_symbols

    except Exception as e:
        log.record_error(f"Error in categorize tickers batch run: {e}")
        raise e