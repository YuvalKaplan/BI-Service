import atexit
from modules.init.exit import cleanup
from modules.object import provider
from modules.parse.url import scrape_provider

atexit.register(cleanup)

if __name__ == '__main__':
    try:
        provider_id = 1
        p = provider.fetch_by_id(provider_id)
        if p and p.url_start:
            files = scrape_provider(p)
            
            print("Following files from scrapping:")
            for f in files:
                print(f"{f.filename}")

            
    except Exception as e:
        print(f"Error in processing source: {e}")
