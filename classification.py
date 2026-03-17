import atexit
import log
import os
from datetime import datetime, timezone
from modules.bt.object.exit import cleanup
from modules.bt.actions.classification import get_classifier


atexit.register(cleanup)

if __name__ == '__main__':
    try:
        print("Starting classification")
        
        ENV_TYPE = os.environ.get("ENV_TYPE")
        environment = ENV_TYPE if ENV_TYPE is not None and ENV_TYPE == 'production' else 'development'
        log.record_status(f"Starting classification in Environment: {environment}")

        start_time = datetime.now(timezone.utc)
        
        try:
            classifier = get_classifier(update_training_set=False)

            results = classifier.classify_symbols([
                "A",
                "AA",
                "AAL",
                "AAMI",
                "AAOI",
                "AAON",
                "AAP",
                "AAPL",
                "AAT",
                "ABBV",
                "ABCB",
                "ABG",
                "ABM",
                "ABNB"
            ])

            print(results)

        except Exception as e:
            print(f"Best Ideas classification failed with error:\n{e}\n\n")
            raise e

        end = datetime.now(timezone.utc)
        message_full = f"Activated at {start_time.strftime("%H:%M:%S")}\nCompleted at {end.strftime("%H:%M:%S")}.\n\n"

    except Exception as e:
        log.record_error(f"Error in Best Ideas stock classification run: {e}")
        raise Exception(f"Categorization Download failed - {e}")
    