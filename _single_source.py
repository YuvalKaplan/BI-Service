import atexit
from modules.init.exit import cleanup
from modules.object import collect_source
from modules.parse.url import scrape_page

atexit.register(cleanup)

if __name__ == '__main__':
    try:
        collect_source_id = 128
        cs = collect_source.fetch_by_id(collect_source_id)
        if cs and cs.url and cs.scrape_levels :
            files = scrape_page(url=cs.url, scrape_levels=cs.scrape_levels, wait_on_selector=cs.wait_on_selector, content_selector=cs.content_selector, events=cs.events)
            
            print("Following files from scrapping:")
            for f in files:
                print(f"{f.filename}")

            
    except Exception as e:
        print(f"Error in processing source: {e}")
