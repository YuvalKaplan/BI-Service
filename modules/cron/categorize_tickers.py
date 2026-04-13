import log
from datetime import date, datetime
from modules.object import batch_run
from modules.object import provider, categorize_etf, categorize_etf_holding, ticker, categorize_ticker
from modules.parse.url import scrape_categorizer
from modules.parse.convert import load, get_tickers
from modules.calc import classification


def download_data():
    """Scrape ETF holdings and refresh the factor cache."""
    try:
        batch_run_id = batch_run.insert(batch_run.BatchRun('categorize_download', 'auto'))

        # --- Style (Growth/Value) ETFs ---
        style_etfs = categorize_etf.fetch_all('style')
        log.record_status(f"Monthly update: processing {len(style_etfs)} style ETFs.")

        for etf in style_etfs:
            d = scrape_categorizer(etf)
            log.record_status(f"Processing '{etf.name}' ETF for categorization.")
            if etf.id and d.etf.file_format and d.etf.mapping and d.file_name:
                map = provider.getMappingFromJson(d.etf.mapping)
                full_rows = load(etf_name=d.etf.name, file_format=d.etf.file_format, mapping=map, file_name=d.file_name, raw_data=d.data)
                symbols = get_tickers(full_rows=full_rows, mapping=map)
                categorize_etf_holding.insert_holding(etf.id, date.today(), symbols)
                categorize_etf.update_last_download(etf.id)

        # --- Build for Classification model
        categorize_ticker.sync_categorize_ticker()
        ct_symbols = categorize_ticker.fetch_symbols()
        factor_updates = classification.update_factor_cache(ct_symbols)
        categorize_ticker.bulk_update(factor_updates)

        # --- ESG ETFs ---
        esg_etfs = categorize_etf.fetch_all('esg')
        log.record_status(f"Monthly update: processing {len(esg_etfs)} ESG ETFs.")
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
            categorize_ticker.update_esg_qualified(list(esg_symbols))
            log.record_status(f"Marked {len(esg_symbols)} categorize_ticker rows as ESG qualified.")

        batch_run.update_completed_at(batch_run_id)
        log.record_status(f"Finished monthly categorize ETF update.\n")
        return len(style_etfs) + len(esg_etfs)

    except Exception as e:
        log.record_error(f"Error in monthly categorize ETF update: {e}")
        raise e


def run_classification():
    """Run style classification against the ticker table."""
    try:
        batch_run_id = batch_run.insert(batch_run.BatchRun('classify_tickers', 'auto'))

        # all_categorized = categorize_ticker.fetch_all_for_style_classification()
        # if not all_categorized:
        #     raise Exception("Categorize_ticker data is empty - cannot use classification model.")

        # categorized_tickers = [classification.to_categorize_ticker_item(t) for t in all_categorized]
        # classifier = classification.get_classifier(categorized_tickers)
        # num_classified = ticker.mark_style(classifier)
        # log.record_status(f"Categorized {num_classified} tickers.\n")

        esg_tickers = categorize_ticker.fetch_all_for_esg()
        if esg_tickers:
            ticker.update_esg_qualified([t.symbol for t in esg_tickers])
            log.record_status(f"Updated {len(esg_tickers)} tickers as ESG qualified.")

        batch_run.update_completed_at(batch_run_id)

    except Exception as e:
        log.record_error(f"Error in categorize tickers classification run: {e}")
        raise e
