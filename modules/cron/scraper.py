import log
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, wait
from modules.object import batch_run, batch_run_log
from modules.parse.scrape import process_source
from modules.object import collect_source

MAX_PER_RUN = 50
PAGE_SIZE = 50
MAX_WORKERS = 10

def run(start_time: datetime) -> tuple[str, list[int] | None]:
    try:
        batch_run_id = None
        if batch_run_id is None:
            batch_run_id = batch_run.insert(batch_run.BatchRun('scraper', 'auto'))

        to_scrape = collect_source.fetch_for_scraping(limit=MAX_PER_RUN)

        log.record_status(f"Running scraper batch job ID {batch_run_id} - will proccess {len(to_scrape)} items.")

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
                    futures = [executor.submit(process_source, item) for item in current_batch]
                    wait(futures)

                    batch_run_log.insert(batch_run_log.BatchRunLog(batch_run_id=batch_run_id, note=f"Page {str(page_number)} with {len(to_scrape)} items"))
                    completed += len(group)

        batch_run.update_completed_at(batch_run_id)

        scrape_ids = [s.id for s in to_scrape if s.id is not None]
        stats = collect_source.get_collection_stats(scrape_ids, start_time)
        stats_scraper = ""
        for line in stats:
            stats_scraper += (f"{line['count']}\t{line['domain']}\n")

        log.record_status(f"Finished scraper batch run on {completed} items.\n{stats_scraper}")
        return stats_scraper, scrape_ids

    except Exception as e:
        log.record_error(f"Error in scraper batch run: {e}")
        raise e