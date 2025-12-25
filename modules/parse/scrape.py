import log
from modules.core import sender
from modules.core.util import get_domain_from_url
from modules.parse.url import scrape_page
from modules.object import collect_source

def process_source(item: collect_source.CollectSource):
    if item.id is None or item.url is None:
        raise Exception("Missing data in DB for scraping.")
        
    try:
        if item.domain is None:
            # Generate the domain from the URL:
            item.domain = get_domain_from_url(item.url)
            collect_source.update_domain(item)

    except Exception as e:
        sender.send_admin(subject=f"Failed to get Domain from URL", message=f"Failed to parse web age URL {item.url} and get the domain.")
        raise Exception(f"Failed to get Domain from URL.")
    
    try:
        log.record_status(f"Starting to collect files from URL '{item.url}'")

        # This is where all the important stuff happens
        files = scrape_page(url=item.url, scrape_levels=item.scrape_levels, wait_on_selector=item.wait_on_selector, content_selector=item.content_selector, events=item.events)

        if len(files) == 0:
            log.record_notice(f"No files identified when scraping URL '{item.url}'")
            collect_source.update_last_scrape(item)
            return

        log.record_status(f"Storing {len(files)} files for analysis of from URL '{item.domain}' and related pages.")

        for file in files:
            print(f"{file.filename}")
           
        log.record_status(f"Completed collection for the domain '{item.domain}'")

    except Exception as e:
        message=f"The processing of the URL '{item.url}' has not completed for domain '{item.domain}'. {e}"
        log.record_error(message)
        sender.send_admin(subject=f"Failed file collection", message=f"{message}")
        raise Exception(message)

    finally:
        collect_source.update_last_scrape(item)
