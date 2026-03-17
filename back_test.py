import atexit
import log
import os
from datetime import date, datetime, timezone
from modules.bt.object.exit import cleanup
from modules.bt.simulation import orchestrator


atexit.register(cleanup)

if __name__ == '__main__':
    try:
        print("Starting back testing run")
        
        ENV_TYPE = os.environ.get("ENV_TYPE")
        environment = ENV_TYPE if ENV_TYPE is not None and ENV_TYPE == 'production' else 'development'
        log.record_status(f"Starting back testing service in Environment: {environment}")

        start_time = datetime.now(timezone.utc)
        
        try:
            results = orchestrator.run(date(2025,1,1), date(2025,12,31))
        except Exception as e:
            print(f"Best Ideas Cron Failed with error:\n{e}\n\n")
            raise e

        end = datetime.now(timezone.utc)
        message_full = f"Activated at {start_time.strftime("%H:%M:%S")}\nCompleted at {end.strftime("%H:%M:%S")}.\n\n"

    except Exception as e:
        log.record_error(f"Error in Best Ideas Back Test run: {e}")
        raise Exception(f"Back Test Job Best Ideas failed - {e}")
    