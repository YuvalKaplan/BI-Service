import log
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, wait
from modules.object import batch_run, batch_run_log
from modules.parse.download import process_provider
from modules.object import provider

PAGE_SIZE = 50
MAX_WORKERS = 10

def run(start_time: datetime) -> tuple[str, list[int] | None]:
    try:
        batch_run_id = None
        if batch_run_id is None:
            batch_run_id = batch_run.insert(batch_run.BatchRun('downloader', 'auto'))

        to_scrape = provider.fetch_active_providers()

        log.record_status(f"Running downloader batch job ID {batch_run_id} - will proccess {len(to_scrape)} items.")

        completed = 0
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            page_number = 0
            has_more_data = True
            batch_count = 0

            while has_more_data:
                batch_count += 1
                current_batch = []

                start = page_number*PAGE_SIZE
                end = start + PAGE_SIZE
                if end > len(to_scrape):
                    group = to_scrape[start:]
                else:
                    group = to_scrape[start:end]

                page_number += 1

                if len(group) == 0:
                    has_more_data = False
                    break

                current_batch.extend(group)

                if current_batch:
                    futures = [executor.submit(process_provider, item) for item in current_batch]
                    wait(futures)

                    batch_run_log.insert(batch_run_log.BatchRunLog(batch_run_id=batch_run_id, note=f"Page {str(page_number)} with {len(to_scrape)} items"))
                    completed += len(group)

        batch_run.update_completed_at(batch_run_id)

        provider_ids = [s.id for s in to_scrape if s.id is not None]
        stats = provider.get_collection_stats(provider_ids, start_time)
        stats_downloader = ""
        for line in stats:
            if line['downloaded'] == line['available']:
                stats_downloader += (f"All\t{line['name']}\n")
            else:
                stats_downloader += (f"{line['downloaded']} out of {line['available']}\t{line['name']}\n")

        log.record_status(f"Finished downloader batch run on {completed} items.\n{stats_downloader}")
        return stats_downloader, provider_ids

    except Exception as e:
        log.record_error(f"Error in downloader batch run: {e}")
        raise e