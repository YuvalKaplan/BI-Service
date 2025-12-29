import log
from modules.core import sender
from modules.core.util import get_domain_from_url
from modules.object.provider import Provider, update_domain
from modules.object.provider_etf import update_last_download
from modules.object.provider_etf_holding import insert_all_holdings

from modules.parse.url import scrape_provider
from modules.parse.convert import transform

def process_provider(provider: Provider):
    if provider.id is None or provider.url_start is None:
        raise Exception("Missing data in DB for provider scraping.")
        
    try:
        if provider.domain is None:
            # Generate the domain from the URL:
            provider.domain = get_domain_from_url(provider.url_start)
            update_domain(provider)

    except Exception as e:
        sender.send_admin(subject=f"Failed to get Domain from URL", message=f"Failed to parse web age URL {provider.url_start} and get the domain.")
        raise Exception(f"Failed to get Domain from URL.")
    
    try:
        log.record_status(f"Starting to collect holdings from provider '{provider.name}'")

        # This is where all the important stuff happens
        downloads = scrape_provider(provider)

        if len(downloads) == 0:
            log.record_notice(f"No holdings downloads identified when scraping URL '{provider.name}'")
            return

        log.record_status(f"Storing holdings for {len(downloads)} etfs for analysis of provider '{provider.name}'.")

        for d in downloads:
            if d.etf and d.etf.id:
                df = transform(d, True)
                insert_all_holdings(d.etf.id, df)
                update_last_download(d.etf.id)

        log.record_status(f"Completed collection for the provider '{provider.name}'")

    except Exception as e:
        message=f"The processing of the provider '{provider.name}' has not completed. {e}"
        log.record_error(message)
        sender.send_admin(subject=f"Failed holdings collection", message=f"{message}")
        raise Exception(message)

