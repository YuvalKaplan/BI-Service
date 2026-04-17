import log
from datetime import date
from modules.bt.object import categorize_etf, categorize_etf_holding, categorize_ticker, provider
from modules.bt.object.categorize_ticker import CategorizeTicker
from modules.parse.url import scrape_categorizer
from modules.parse.convert import load, get_tickers
from modules.calc import classification


def download_data():
    """Scrape categorization ETF holdings and populate the BT categorize_ticker table with style and factors."""
    log.record_status("BT: Starting categorize ETF download.")

    # --- Style (Growth/Value) ETFs → BT categorize_etf_holding + categorize_ticker ---
    style_etfs = categorize_etf.fetch_all('style')
    log.record_status(f"BT: Processing {len(style_etfs)} style ETFs.")

    for etf in style_etfs:
        d = scrape_categorizer(etf)
        log.record_status(f"BT: Processing '{etf.name}' for categorization.")
        if etf.id and d.etf.file_format and d.etf.mapping and d.file_name:
            map = provider.getMappingFromJson(d.etf.mapping)
            full_rows = load(etf_name=d.etf.name, file_format=d.etf.file_format, mapping=map, file_name=d.file_name, raw_data=d.data)
            symbols = get_tickers(full_rows=full_rows, mapping=map)
            categorize_etf_holding.insert_holding(etf.id, date.today(), symbols)
            categorize_etf.update_last_download(etf.id)
            rows = [
                CategorizeTicker(
                    symbol=s,
                    style_type=etf.style_type,
                    cap_type=etf.cap_type,
                    sector="Unknown",
                    market_cap=0,
                    esg_qualified=None,
                    factors={},
                )
                for s in symbols
            ]
            categorize_ticker.upsert_bulk(rows)

    # --- Fetch factors for all BT categorize_ticker symbols ---
    ct_symbols = categorize_ticker.fetch_symbols()
    log.record_status(f"BT: Fetching factors for {len(ct_symbols)} categorize_ticker symbols.")
    factor_updates = classification.update_factor_cache(ct_symbols)
    categorize_ticker.bulk_update_factors(factor_updates)
    log.record_status(f"BT: Factor cache updated for {len(factor_updates)} symbols.")

    # --- ESG ETFs → BT categorize_etf_holding + mark ESG in categorize_ticker ---
    esg_etfs = categorize_etf.fetch_all('esg')
    log.record_status(f"BT: Processing {len(esg_etfs)} ESG ETFs.")
    esg_symbols: set[str] = set()
    for etf in esg_etfs:
        d = scrape_categorizer(etf)
        log.record_status(f"BT: Processing '{etf.name}' ESG ETF.")
        if etf.id and d.etf.file_format and d.etf.mapping and d.file_name:
            map = provider.getMappingFromJson(d.etf.mapping)
            full_rows = load(etf_name=d.etf.name, file_format=d.etf.file_format, mapping=map, file_name=d.file_name, raw_data=d.data)
            symbols = get_tickers(full_rows=full_rows, mapping=map)
            categorize_etf_holding.insert_holding(etf.id, date.today(), symbols)
            categorize_etf.update_last_download(etf.id)
            esg_symbols.update(symbols)
    if esg_symbols:
        categorize_ticker.update_esg_qualified(list(esg_symbols))
        log.record_status(f"BT: Marked {len(esg_symbols)} categorize_ticker rows as ESG qualified.")

    log.record_status("BT: Finished categorize ETF download.")
