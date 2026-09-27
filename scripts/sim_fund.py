import atexit
from datetime import date
from modules.object.exit import cleanup
from modules.sim import orchestrator

atexit.register(cleanup)

if __name__ == '__main__':
    try:
        fund_id = 8  # <-- edit before each run
        inception_date = date(2026, 7, 1)
        weeks = 3  # <-- edit before each run (None = run to present)
        show_holdings = False  # <-- edit before each run (True = also show full holdings w/ %)
        orchestrator.run(fund_id, inception_date, weeks, show_holdings)
    except Exception as e:
        print(f"Error in fund simulation: {e}")
