import atexit
import log
import os
from datetime import datetime, timezone
from modules.init.exit import cleanup
from modules.core.db import db_pool_instance
from modules.core import sender
from modules.cron import downloader, categorize_tickers

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

        if 0 <= start_time.weekday() <= 6: # Monday to Friday
            try:
                stats_downloader, total_downloaded, provider_ids, = downloader.run(start_time)
            except Exception as e:
                sender.send_admin(subject="Best Ideas Cron Failed", message=f"Failed on holdings download with error:\n{e}\n")
                raise e
            
            # Inform admin that batch has completed.
            message_actions += f"Holdings Download\n--------------------------------\n{stats_downloader}\n\n"
            message_actions += f"--------------------------------\n"
            message_actions += f"Total ETFs downloaded: {total_downloaded}\n"

        if start_time.day == 15: # middle of each month
            try:
                total_symbols = categorize_tickers.run()
            except Exception as e:
                sender.send_admin(subject="Best Ideas Cron Failed", message=f"Failed on categorize tickers with error:\n{e}\n")
                raise e
            
            # Inform admin that batch has completed.
            message_actions += f"Categorized tickers: {total_symbols}\n\n"

        end = datetime.now(timezone.utc)
        message_full = f"Activated at {start_time.strftime("%H:%M:%S")}\nCompleted at {end.strftime("%H:%M:%S")}.\n\n"
        sender.send_admin(subject="Best Ideas Cron Completed", message=message_full + message_actions)

    except Exception as e:
        log.record_error(f"Error starting cron service: {e}")
        raise Exception(f"Cron job failed - {e}")
    