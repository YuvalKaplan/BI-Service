import log
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, wait
from modules.object import batch_run, ticker
from modules.parse.download import process_provider
from modules.object import provider

MAX_WORKERS = 5

def run(start_time: datetime) -> tuple[str, int, list[int] | None]:
    try:
        batch_run_id = None
        if batch_run_id is None:
            batch_run_id = batch_run.insert(batch_run.BatchRun('etf_downloader', 'auto'))

        to_scrape = provider.fetch_active_providers()

        log.record_status(f"Running ETF Downloader batch job ID {batch_run_id} - will proccess {len(to_scrape)} items.")

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(process_provider, item) for item in to_scrape]
            wait(futures)

        batch_run.update_completed_at(batch_run_id)

        provider_ids = [s.id for s in to_scrape if s.id is not None]
        stats = provider.get_collection_stats(provider_ids, start_time)
        stats_downloader = ""
        total_downloaded = 0
        for line in stats:
            total_downloaded += line['downloaded']
            if line['downloaded'] == line['available']:
                stats_downloader += "{:<8}{:<20}{}\n".format(line['id'], f"All ({line['available']})", line['name'])
            else:
                stats_downloader += "{:<8}{:<20}{}\n".format(line['id'], f"{line['downloaded']} out of {line['available']}", line['name'])

        log.record_status(f"Finished ETF Downloader batch run on {len(to_scrape)} items.\n{stats_downloader}")

        ticker.sync_tickers_with_etf_holdings()

        return stats_downloader, total_downloaded, provider_ids

    except Exception as e:
        log.record_error(f"Error in downloader batch run: {e}")
        raise e