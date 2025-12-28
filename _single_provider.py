import atexit
from modules.init.exit import cleanup
from modules.object import provider
from modules.parse.url import scrape_provider
from modules.parse.convert import transform

atexit.register(cleanup)

if __name__ == '__main__':
    try:
        provider_id = 1
        p = provider.fetch_by_id(provider_id)
        if p and p.url_start and p.mapping:            
            downloads = scrape_provider(p)
            for d in downloads:
                df = transform(d, True)
                
                print(df.head())
                print("... ------------ ...")
                print(df.tail())

    except Exception as e:
        print(f"Error in scraping and processing single provider: {e}")
