import atexit
import log
import os
from datetime import datetime, timezone
from modules.init.exit import cleanup
from modules.categorization import downloader


atexit.register(cleanup)

if __name__ == '__main__':
    try:
        print("Starting categorization download")
        
        ENV_TYPE = os.environ.get("ENV_TYPE")
        environment = ENV_TYPE if ENV_TYPE is not None and ENV_TYPE == 'production' else 'development'
        log.record_status(f"Starting categorization download in Environment: {environment}")

        start_time = datetime.now(timezone.utc)
        
        try:
            results = downloader.run()
        except Exception as e:
            print(f"Best Ideas categorization download failed with error:\n{e}\n\n")
            raise e

        end = datetime.now(timezone.utc)
        message_full = f"Activated at {start_time.strftime("%H:%M:%S")}\nCompleted at {end.strftime("%H:%M:%S")}.\n\n"

    except Exception as e:
        log.record_error(f"Error in Best Ideas categorization download run: {e}")
        raise Exception(f"Categorization Download failed - {e}")
    