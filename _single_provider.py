import atexit
from modules.init.exit import cleanup
from modules.object import provider, provider_etf_holding
from modules.parse.url import scrape_provider
from modules.parse.convert import transform
from modules.parse.download import process_provider

atexit.register(cleanup)

if __name__ == '__main__':
    try:
        provider_id = 55
        p = provider.fetch_by_id(provider_id)
        if p and p.url_start:
            # process_provider(p)         
            downloads = scrape_provider(p)
            for d in downloads:
                if d.etf and d.etf.id:
                    try:
                        df = transform(d, True)
                        print(f"{d.etf.name}\t{d.file_name}")
                        print(df.head())
                        print("... ------------ ...")
                        print(df.tail())
                        provider_etf_holding.insert_all_holdings(d.etf.id, df)
                    except Exception as e:
                        print(e)

    except Exception as e:
        print(f"Error in scraping and processing single provider: {e}")
