import atexit
import os
from modules.object.exit import cleanup
from modules.object import provider, provider_etf
from modules.object.provider import getMappingFromJson
from modules.parse.convert import load, map_data
from modules.ticker.resolver import TickerResolver

atexit.register(cleanup)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '..', '.downloads')

if __name__ == '__main__':
    try:
        provider_etf_id = 38
        filename = 'JPMorgan-Nasdaq-Equity-Premium-Income-ETF-ETF-Shares-Holdings-04-20-2026.xlsx'

        etf = provider_etf.fetch_by_id(provider_etf_id)
        p = provider.fetch_by_id(etf.provider_id)
        if not p:
            raise Exception(f"No provider found with id={etf.provider_id}")

        file_format = etf.file_format or p.file_format
        use_mapping = etf.mapping or p.mapping
        if not file_format or not use_mapping:
            raise Exception('Missing file type or mapping information in database for data transformation.')

        file_path = os.path.join(OUTPUT_DIR, filename)
        with open(file_path, 'rb') as f:
            raw_data = f.read()

        mapping = getMappingFromJson(use_mapping)
        full_rows = load(etf_name=etf.name, file_format=file_format, mapping=mapping, file_name=filename, raw_data=raw_data)
        df = map_data(full_rows=full_rows, file_name=filename, date_from_page=None, mapping=mapping)

        resolver = TickerResolver(TickerResolver.POPULATE_TICKER)
        df['ticker_id'] = df.apply(
            lambda row: resolver.resolve(
                region=etf.region,
                symbol=row.get('ticker'),
                isin=row.get('isin'),
                name=row.get('name'),
            ),
            axis=1
        )

        print(df.head())
        print("... ------------ ...")
        print(df.tail())

    except Exception as e:
        print(f"Error in converting single read: {e}")
