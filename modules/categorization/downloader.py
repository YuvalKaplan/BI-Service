import log
from datetime import datetime, timezone
from modules.object import provider, categorize_etf, categorize_etf_holding
from modules.parse.url import scrape_categorizer
from modules.parse.convert import load, get_tickers

def run():
    try:
        etfs = categorize_etf.fetch_all()
        
        log.record_status(f"Running Categorize Tickers download.")

        for etf in etfs:
            d = scrape_categorizer(etf)
            log.record_status(f"Processing '{etf.name}' ETF for categorization.")
            if etf.id and d.etf.file_format and d.etf.mapping and d.file_name:
                map = provider.getMappingFromJson(d.etf.mapping)
                full_rows = load(etf_name=d.etf.name, file_format=d.etf.file_format, mapping=map, file_name=d.file_name, raw_data=d.data)
                symbols = get_tickers(full_rows=full_rows, mapping=map)
                
                if etf.cap_type and etf.style_type:
                    categorize_etf_holding.insert_holding(etf.id, datetime.now(timezone.utc), symbols)
                    
                categorize_etf.update_last_download(etf.id)

        log.record_status(f"Finished Categorize Tickers download {len(etfs)} ETFs.\n")
        return

    except Exception as e:
        log.record_error(f"Error in categorize tickers download: {e}")
        raise e