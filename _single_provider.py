import atexit
from modules.init.exit import cleanup
from modules.object import provider, provider_etf_holding 
from modules.parse.url import scrape_provider
from modules.parse.convert import load, map_data
from modules.parse.download import process_provider

atexit.register(cleanup)

if __name__ == '__main__':
    try:
        provider_id = 14
        p = provider.fetch_by_id(provider_id)
        if p and p.url_start:
            # process_provider(p)         
            downloads = scrape_provider(p)
            for d in downloads:
                try:
                    file_format = d.etf.file_format or d.provider.file_format
                    mapping  = d.etf.mapping or d.provider.mapping
                    if d.etf.id and file_format and mapping and d.file_name:
                        map = provider.getMappingFromJson(mapping)
                        full_rows = load(etf_name=d.etf.name, file_format=file_format, mapping=map, file_name=d.file_name, raw_data=d.data)
                        df = map_data(full_rows=full_rows, file_name=d.file_name, mapping=map)
                        print(f"{d.etf.name}\t{d.file_name}")
                        print(df.head())
                        print("... ------------ ...")
                        print(df.tail())
                        provider_etf_holding.insert_all_holdings(d.etf.id, df)
                except Exception as e:
                    print(e)

    except Exception as e:
        print(f"Error in scraping and processing single provider: {e}")
