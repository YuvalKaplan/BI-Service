import log
from datetime import date
from modules.object import batch_run
from modules.object import categorize_etf
from modules.object import categorize_etf_holding
from modules.parse.url import scrape_categorizer
from modules.parse.convert import load, map_data
from modules.object.provider import getMappingFromJson
from modules.ticker.resolver import TickerResolver


def run() -> int:
    """Scrape categorization ETF holdings and populate categorize_ticker (style value/growth)."""
    try:
        batch_run_id = batch_run.insert(batch_run.BatchRun(process='categorize_download', activation='auto'))

        style_etfs = categorize_etf.fetch_all('style')
        log.record_status(f"Style (Growth/Value) update: processing {len(style_etfs)} style ETFs.")

        resolver = TickerResolver(TickerResolver.POPULATE_CATEGORY_TICKER)
        for etf in style_etfs:
            d = scrape_categorizer(etf)
            log.record_status(f"Processing '{etf.name}' ETF for categorization.")
            if etf.id and etf.style_type and etf.cap_type and d.etf.file_format and d.etf.mapping and d.file_name:
                map_obj = getMappingFromJson(d.etf.mapping)
                full_rows = load(
                    etf_name=d.etf.name, file_format=d.etf.file_format,
                    mapping=map_obj, file_name=d.file_name, raw_data=d.data,
                )
                df = map_data(
                    full_rows=full_rows, file_name=d.file_name,
                    date_from_page=d.date_from_page, mapping=map_obj,
                )
                log.record_status(f"Resolving tickers for ETF '{etf.name}'...")
                resolver.set_classification(etf.style_type, etf.cap_type)
                df['cat_ticker_id'] = df.apply(
                    lambda row: resolver.resolve(
                        region=etf.region or 'US',
                        symbol=row.get('ticker'),
                        isin=row.get('isin'),
                        name=row.get('name'),
                    ),
                    axis=1,
                )
                cat_ticker_ids = df['cat_ticker_id'].dropna().astype(int).tolist()
                categorize_etf_holding.insert_holding(etf.id, date.today(), cat_ticker_ids)
                categorize_etf.update_last_download(etf.id)
                resolved = int(df['cat_ticker_id'].notna().sum())
                log.record_status(
                    f"ETF '{etf.name}': {resolved}/{len(df)} holdings resolved into categorize_ticker."
                )

        batch_run.update_completed_at(batch_run_id)
        log.record_status(f"Finished categorize ETF update.\n")
        return len(style_etfs)

    except Exception as e:
        log.record_error(f"Error in categorize ETF update: {e}")
        raise e
