import log
from datetime import date
from modules.object import batch_run
from modules.object import provider, categorize_etf, categorize_etf_holding, ticker
from modules.parse.url import scrape_categorizer
from modules.parse.convert import load, get_tickers
from modules.object.ticker import fetch_by_symbols


def run() -> int:
    """Scrape categorization ETF holdings and update the ticker table [style (value/growth)]."""
    try:
        batch_run_id = batch_run.insert(batch_run.BatchRun(process='categorize_download', activation='auto'))

        # --- Style (Growth/Value) → categorize_etf_holding ---
        style_etfs = categorize_etf.fetch_all('style')
        log.record_status(f"Style (Growth/Value) update: processing {len(style_etfs)} style ETFs.")

        for etf in style_etfs:
            d = scrape_categorizer(etf)
            log.record_status(f"Processing '{etf.name}' ETF for categorization.")
            if etf.id and d.etf.file_format and d.etf.mapping and d.file_name:
                map = provider.getMappingFromJson(d.etf.mapping)
                full_rows = load(etf_name=d.etf.name, file_format=d.etf.file_format, mapping=map, file_name=d.file_name, raw_data=d.data)
                symbols = get_tickers(full_rows=full_rows, mapping=map)
                tickers = fetch_by_symbols(symbols)
                ticker_ids = [t.id for t in tickers if t.id is not None and not t.invalid]
                categorize_etf_holding.insert_holding(etf.id, date.today(), ticker_ids)
                categorize_etf.update_last_download(etf.id)

        # --- Style (Growth/Value) -> ticker table ---
        ticker.update_style_from_categorization_etfs()
        log.record_status("Updated ticker style_type/cap_type from categorization ETF holdings.")

        batch_run.update_completed_at(batch_run_id)
        log.record_status(f"Finished categorize ETF update.\n")
        return len(style_etfs)

    except Exception as e:
        log.record_error(f"Error in categorize ETF update: {e}")
        raise e
