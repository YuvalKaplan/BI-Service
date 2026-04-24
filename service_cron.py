import atexit
import log
import os
from datetime import datetime, timezone
from modules.object.exit import cleanup
from modules.core.db import db_pool_instance
from modules.core import sender
from modules.calc.model_fund import results_to_string
from modules.cron import etf_downloader, best_ideas_generator, funds_update, stocks_categorize, esg_update
from modules.object import ticker

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
        weekday = start_time.weekday() # 0 = Monday, 4 = Friday, 6 = Sunday
        message_actions = ""

        if 1 <= weekday <= 6: # Tuesday through Saturday
            try:
                stats_downloader, total_downloaded, provider_ids, = etf_downloader.run(start_time)
            except Exception as e:
                sender.send_admin(subject="Best Ideas Cron Failed", message=f"Failed on holdings download with error:\n{e}\n")
                raise e
            
            message_actions += f"Holdings Download\n" + SEPERATOR_LINE
            message_actions += f"{stats_downloader}\n" + SEPERATOR_LINE
            message_actions += f"Total ETFs downloaded: {total_downloaded}\n"
            message_actions += BREAKER_LINE

        # if weekday == 6:  # Sunday
        #     try:
        #         total_etfs = stocks_categorize.run()
        #     except Exception as e:
        #         sender.send_admin(subject="Best Ideas Cron Failed", message=f"Failed on categorize stocks with error:\n{e}\n\n")
        #         raise e

        #     message_actions += f"Categorization ETFs processed: {total_etfs}\n"
        #     message_actions += BREAKER_LINE

        #     try:
        #         total_esg = esg_update.run()
        #     except Exception as e:
        #         sender.send_admin(subject="Best Ideas Cron Failed", message=f"Failed on ESG update with error:\n{e}\n\n")
        #         raise e

        #     message_actions += f"ESG tickers refreshed: {total_esg}\n"
        #     message_actions += BREAKER_LINE

        # if 1 <= weekday <= 6: # Tuesday through Saturday
        #     try:
        #         etfs_processed, generated_etfs, problems = best_ideas_generator.run()
        #     except Exception as e:
        #         sender.send_admin(subject="Best Ideas Cron Failed", message=f"Failed on best ideas processing with error:\n{e}\n\n")
        #         raise e
            
        #     message_actions += f"Total ETFs available: {etfs_processed}\n"
        #     message_actions += f"ETFS with best ideas: {generated_etfs}\n"
        #     message_actions += f"ETFS with problems: {len(problems)}\n" + SEPERATOR_LINE
        #     for p in problems:
        #         message_actions += f"{p}\n"
        #     message_actions += BREAKER_LINE

        #     try:
        #         results = funds_update.run()
        #     except Exception as e:
        #         sender.send_admin(subject="Best Ideas Cron Failed", message=f"Failed on model fund update with error:\n{e}\n\n")
        #         raise e

        #     message_actions += f"Fund Updates:\n" + SEPERATOR_LINE
        #     for r in results:
        #         message_actions += f"{results_to_string(r, ticker)}\n"
        #         message_actions += BREAKER_LINE

        # end = datetime.now(timezone.utc)
        # message_full = f"Activated at {start_time.strftime("%H:%M:%S")}\nCompleted at {end.strftime("%H:%M:%S")}.\n\n"
        # sender.send_admin(subject="Best Ideas Cron Completed", message=message_full + message_actions)

    except Exception as e:
        log.record_error(f"Error in Best Ideas cron service: {e}")
        raise Exception(f"Cron Job Best Ideas failed - {e}")
    