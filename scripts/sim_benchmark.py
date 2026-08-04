import atexit
from datetime import date
from modules.object.exit import cleanup
from modules.sim import benchmark_generator

atexit.register(cleanup)

if __name__ == '__main__':
    try:
        inception_date = date(2026, 1, 15)  # <-- edit before each run
        benchmark_generator.run(inception_date)
    except Exception as e:
        print(f"Error in simulated benchmark backfill: {e}")
