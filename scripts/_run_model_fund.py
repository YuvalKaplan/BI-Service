import atexit
from modules.object.exit import cleanup
from modules.cron.funds_update import run

atexit.register(cleanup)

if __name__ == '__main__':
    try:
        results = run()

    except Exception as e:
        print(f"Error in fund updater test: {e}")
