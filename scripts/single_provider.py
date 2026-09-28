import atexit
from modules.object.exit import cleanup
from modules.object import provider, provider_etf_holding, ticker
from modules.parse.url import scrape_provider
from modules.parse.convert import load, map_data
from modules.parse.download import process_provider
from modules.ticker import pricing, valuation
from modules.ticker.resolver import TickerResolver

atexit.register(cleanup)

if __name__ == '__main__':
    try:
        provider_id = 39
        p = provider.fetch_by_id(provider_id)
        if p and p.url_start:
            # process_provider(p)
            downloads = scrape_provider(p)
            resolver = TickerResolver(TickerResolver.POPULATE_TICKER)
            resolved_ids: set[int] = set()
            for d in downloads:
                try:
                    file_format = d.etf.file_format or d.provider.file_format
                    mapping  = d.etf.mapping or d.provider.mapping
                    if d.etf.id and file_format and mapping and d.file_name:
                        map = provider.getMappingFromJson(mapping)
                        full_rows = load(etf_name=d.etf.name, file_format=file_format, mapping=map, file_name=d.file_name, raw_data=d.data)
                        df = map_data(full_rows=full_rows, file_name=d.file_name, date_from_page=d.date_from_page, mapping=map)
                        df['ticker_id'] = df.apply(
                            lambda row: resolver.resolve(
                                region=d.etf.region,
                                symbol=row.get('ticker'),
                                isin=row.get('isin'),
                                name=row.get('name'),
                            ),
                            axis=1
                        )
                        df = df[df['ticker_id'].notna()]
                        print(f"{d.etf.name}\t{d.file_name}")
                        print(df.head())
                        print("... ------------ ...")
                        print(df.tail())
                        provider_etf_holding.insert_all_holdings(d.etf.id, df)
                        resolved_ids.update(int(i) for i in df['ticker_id'].unique())
                except Exception as e:
                    print(e)

            # Resolution registers the tickers; their values come from the valuation pass.
            values = valuation.store_values(
                valuation.targets_for_tickers(ticker.fetch_by_ids(list(resolved_ids))), pricing.latest_value_date())
            print(f"Values: {values.validated} validated, {values.already_valued} already valued, {values.withheld} withheld")

    except Exception as e:
        print(f"Error in scraping and processing single provider: {e}")
