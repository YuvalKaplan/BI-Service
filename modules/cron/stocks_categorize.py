import log
from datetime import date
from modules.object import batch_run
from modules.object import provider, categorize_etf, categorize_etf_holding, ticker
from modules.parse.url import scrape_categorizer
from modules.parse.convert import load, get_tickers


def download_data() -> int:
    """Scrape categorization ETF holdings and update the ticker table [style (value/growth) and ESG]."""
    try:
        batch_run_id = batch_run.insert(batch_run.BatchRun('categorize_download', 'auto'))

        # --- Style (Growth/Value) ETFs → categorize_etf_holding ---
        style_etfs = categorize_etf.fetch_all('style')
        log.record_status(f"Style (Growth/Value) update: processing {len(style_etfs)} style ETFs.")

        for etf in style_etfs:
            d = scrape_categorizer(etf)
            log.record_status(f"Processing '{etf.name}' ETF for categorization.")
            if etf.id and d.etf.file_format and d.etf.mapping and d.file_name:
                map = provider.getMappingFromJson(d.etf.mapping)
                full_rows = load(etf_name=d.etf.name, file_format=d.etf.file_format, mapping=map, file_name=d.file_name, raw_data=d.data)
                symbols = get_tickers(full_rows=full_rows, mapping=map)
                categorize_etf_holding.insert_holding(etf.id, date.today(), symbols)
                categorize_etf.update_last_download(etf.id)

        # --- Style (Growth/Value) -> ticker table ---
        ticker.update_style_from_categorization_etfs()
        log.record_status("Updated ticker style_type/cap_type from categorization ETF holdings.")

        ticker.update_style_from_provider_etfs()
        log.record_status("Updated remaining ticker style_type from provider ETF holdings.")

        # --- ESG ETFs → ticker table ---
        esg_etfs = categorize_etf.fetch_all('esg')
        log.record_status(f"ESG update: processing {len(esg_etfs)} ESG ETFs.")
        esg_symbols: set[str] = set()
        for etf in esg_etfs:
            d = scrape_categorizer(etf)
            log.record_status(f"Processing '{etf.name}' ESG ETF.")
            if etf.id and d.etf.file_format and d.etf.mapping and d.file_name:
                map = provider.getMappingFromJson(d.etf.mapping)
                full_rows = load(etf_name=d.etf.name, file_format=d.etf.file_format, mapping=map, file_name=d.file_name, raw_data=d.data)
                symbols = get_tickers(full_rows=full_rows, mapping=map)
                esg_symbols.update(symbols)
        if esg_symbols:
            ticker.update_esg_qualified(list(esg_symbols))
            log.record_status(f"Marked {len(esg_symbols)} tickers as ESG qualified.")

        batch_run.update_completed_at(batch_run_id)
        log.record_status(f"Finished categorize ETF update.\n")
        return len(style_etfs) + len(esg_etfs)

    except Exception as e:
        log.record_error(f"Error in categorize ETF update: {e}")
        raise e
