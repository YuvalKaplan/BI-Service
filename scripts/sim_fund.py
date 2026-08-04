import atexit
from datetime import date
from modules.object.exit import cleanup
from modules.sim import orchestrator

atexit.register(cleanup)

if __name__ == '__main__':
    try:
        fund_id = 1  # <-- edit before each run
        inception_date = date(2026, 5, 1)
        orchestrator.run(fund_id, inception_date)
    except Exception as e:
        print(f"Error in fund simulation: {e}")
