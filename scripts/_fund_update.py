import atexit
from modules.object.exit import cleanup
from modules.calc.model_fund import results_to_string
from modules.cron.funds_update import run, results_to_string
from modules.object import ticker


atexit.register(cleanup)

if __name__ == '__main__':
    try:
        results = run()
        print(results_to_string(results, ticker))

    except Exception as e:
        print(f"Error in fund updater test: {e}")
