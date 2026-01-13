import log
from modules.core import sender
from modules.core.util import get_domain_from_url
from modules.object.provider import Provider, update_domain, getMappingFromJson
from modules.object.provider_etf import update_last_download
from modules.object.provider_etf_holding import insert_all_holdings

from modules.parse.url import scrape_provider
from modules.parse.convert import load, map_data

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

        log.record_status(f"Storing holdings for {len(downloads)} ETFs for analysis of provider '{provider.name}'.")

        for d in downloads:
            if d.etf and d.etf.id:
                try:
                    file_format = d.etf.file_format or d.provider.file_format
                    mapping  = d.etf.mapping or d.provider.mapping
                    if mapping and d.file_name:
                        map = getMappingFromJson(mapping)
                        full_rows = load(etf_name=d.etf.name, file_format=file_format, mapping=map, file_name=d.file_name, raw_data=d.data)
                        df = map_data(full_rows=full_rows, file_name=d.file_name, mapping=map)
                        insert_all_holdings(d.etf.id, df)
                        update_last_download(d.etf.id)

                except Exception as e:
                    log.record_error(f"Failed to parse the data for ETF '{d.etf.name}'. {e}")

        log.record_status(f"Completed collection for the provider '{provider.name}'")

    except Exception as e:
        message=f"The processing of the provider '{provider.name}' has not completed. {e}"
        log.record_error(message)
        sender.send_admin(subject=f"Failed holdings collection", message=f"{message}")
        raise Exception(message)

