import atexit
import os
from modules.object.exit import cleanup
from modules.object import provider, provider_etf, provider_etf_holding
from modules.parse.url import scrape_provider_etf
from modules.parse.convert import load, map_data
from modules.ticker.resolver import resolve

atexit.register(cleanup)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '..', '.downloads')

if __name__ == '__main__':
    try:
        provider_etf_id = 38
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
        if etf.id and file_format and mapping:
            map_obj = provider.getMappingFromJson(mapping)
            full_rows = load(etf_name=etf.name, file_format=file_format, mapping=map_obj, file_name=d.file_name, raw_data=d.data)
            df = map_data(full_rows=full_rows, file_name=d.file_name, date_from_page=d.date_from_page, mapping=map_obj)
            df['ticker_id'] = df.apply(
                lambda row: resolve(
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

    except Exception as e:
        print(f"Error in scraping single provider ETF: {e}")
