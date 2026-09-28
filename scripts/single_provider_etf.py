import atexit
import os
from modules.object.exit import cleanup
from modules.object import provider, provider_etf, provider_etf_holding, ticker
from modules.parse.url import scrape_provider_etf
from modules.parse.convert import load, map_data
from modules.ticker import pricing, valuation
from modules.ticker.resolver import TickerResolver

atexit.register(cleanup)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '..', '.output', 'downloads')

if __name__ == '__main__':
    try:
        provider_etf_id = 120
        etf = provider_etf.fetch_by_id(provider_etf_id)
        p = provider.fetch_by_id(etf.provider_id)
        if not p:
            raise Exception(f"No provider found with id={etf.provider_id}")

        d = scrape_provider_etf(p, etf)

        if not d.file_name or not d.data:
            raise Exception("Download returned no file.")
        
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        out_path = os.path.join(OUTPUT_DIR, d.file_name)
        with open(out_path, 'wb') as f:
            f.write(d.data)
        print(f"Saved: {out_path}")

        file_format = etf.file_format or p.file_format
        mapping = etf.mapping or p.mapping
        resolver = TickerResolver(TickerResolver.POPULATE_TICKER)
        if etf.id and file_format and mapping:
            map_obj = provider.getMappingFromJson(mapping)
            full_rows = load(etf_name=etf.name, file_format=file_format, mapping=map_obj, file_name=d.file_name, raw_data=d.data)
            df = map_data(full_rows=full_rows, file_name=d.file_name, date_from_page=d.date_from_page, mapping=map_obj)
            df['ticker_id'] = df.apply(
                lambda row: resolver.resolve(
                    region=etf.region,
                    symbol=row.get('ticker'),
                    isin=row.get('isin'),
                    name=row.get('name'),
                ),
                axis=1
            )
            df = df[df['ticker_id'].notna()]
            print(f"{etf.name}\t{d.file_name}")
            print(df.head())
            print("... ------------ ...")
            print(df.tail())
            provider_etf_holding.insert_all_holdings(etf.id, df)
            # Resolution registers the tickers; their values come from the valuation pass.
            values = valuation.store_values(
                valuation.targets_for_tickers(ticker.fetch_by_ids([int(i) for i in df['ticker_id'].unique()])),
                pricing.latest_value_date())
            print(f"Values: {values.validated} validated, {values.already_valued} already valued, {values.withheld} withheld")

    except Exception as e:
        print(f"Error in scraping single provider ETF: {e}")
