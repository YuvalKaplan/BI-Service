import atexit
import log
import os
from datetime import datetime, timezone
from modules.init.exit import cleanup
from modules.core.db import db_pool_instance
from modules.core import sender
from modules.cron import etf_downloader, categorize_tickers, stock_downloader, best_ideas_generator, funds_update

SEPERATOR_LINE = "-" * 20 + "\n"
BREAKER_LINE = "=" * 20 + "\n\n"

atexit.register(cleanup)

if __name__ == '__main__':
    try:
        print("Starting cron service")
        print(f"DB connection pool started with {db_pool_instance.get_max_connections()} connections.")
        
        ENV_TYPE = os.environ.get("ENV_TYPE")
        environment = ENV_TYPE if ENV_TYPE is not None and ENV_TYPE == 'production' else 'development'
        log.record_status(f"Starting cron service in Environment: {environment}")

        start_time = datetime.now(timezone.utc)
        message_actions = ""

        if 1 <= start_time.weekday() <= 5: # Tuesday through Saturday
            try:
                stats_downloader, total_downloaded, provider_ids, = etf_downloader.run(start_time)
            except Exception as e:
                sender.send_admin(subject="Best Ideas Cron Failed", message=f"Failed on holdings download with error:\n{e}\n")
                raise e
            
            message_actions += f"Holdings Download\n" + SEPERATOR_LINE
            message_actions += f"{stats_downloader}\n" + SEPERATOR_LINE
            message_actions += f"Total ETFs downloaded: {total_downloaded}\n"
            message_actions += BREAKER_LINE

        if start_time.day == 15: # middle of each month - the ETF data is updated once a month on the website
            try:
                total_symbols = categorize_tickers.run()
            except Exception as e:
                sender.send_admin(subject="Best Ideas Cron Failed", message=f"Failed on categorize tickers with error:\n{e}\n\n")
                raise e
            
            message_actions += f"Categorized tickers: {total_symbols}\n"
            message_actions += BREAKER_LINE

        if 0 <= start_time.weekday() <= 6: # All days of the week
            try:
                total_updated, missing_data = stock_downloader.run()
            except Exception as e:
                sender.send_admin(subject="Best Ideas Cron Failed", message=f"Failed on download stock data (price and market cap) with error:\n{e}\n\n")
                raise e
            
            message_actions += f"Stocks updated: {total_updated - missing_data} out of {total_updated}\n"
            message_actions += f"Stocks missing data: {missing_data}\n"
            message_actions += BREAKER_LINE

        if 1 <= start_time.weekday() <= 5: # Tuesday through Saturday
            try:
                etfs_processed, generated_etfs, problems = best_ideas_generator.run()
            except Exception as e:
                sender.send_admin(subject="Best Ideas Cron Failed", message=f"Failed on best ideas processing with error:\n{e}\n\n")
                raise e
            
            message_actions += f"Total ETFs available: {etfs_processed}\n"
            message_actions += f"ETFS with best ideas: {generated_etfs}\n"
            message_actions += f"ETFS with problems: {len(problems)}\n" + SEPERATOR_LINE
            for p in problems:
                message_actions += f"{p}\n"
            message_actions += BREAKER_LINE


        if 1 <= start_time.weekday() <= 5: # Tuesday through Saturday
            try:
                results = funds_update.run()
            except Exception as e:
                sender.send_admin(subject="Best Ideas Cron Failed", message=f"Failed on fund update with error:\n{e}\n\n")
                raise e

            message_actions += f"Fund Updates\n" + SEPERATOR_LINE
            message_actions += f"{funds_update.results_to_string(results)}\n"
            message_actions += BREAKER_LINE

        end = datetime.now(timezone.utc)
        message_full = f"Activated at {start_time.strftime("%H:%M:%S")}\nCompleted at {end.strftime("%H:%M:%S")}.\n\n"
        sender.send_admin(subject="Best Ideas Cron Completed", message=message_full + message_actions)

    except Exception as e:
        log.record_error(f"Error in Best Ideas cron service: {e}")
        raise Exception(f"Cron Job Best Ideas failed - {e}")
    