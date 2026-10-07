import atexit
from modules.object.exit import cleanup
from modules.cron.best_ideas_generator import run, summary


atexit.register(cleanup)

if __name__ == '__main__':
    try:
        print(summary(run()))

    except Exception as e:
        print(f"Error in best_idea generator test: {e}")
