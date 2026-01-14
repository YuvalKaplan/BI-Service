import atexit
from modules.init.exit import cleanup
from modules.cron.best_ideas_generator import run


atexit.register(cleanup)

if __name__ == '__main__':
    try:
        etfs_processed, generated_etfs, problems = run()
        print(f"Total ETFs available: {etfs_processed}")
        print(f"ETFS processed with best ideas: {generated_etfs}")
        print(f"ETFS with problems: {len(problems)}")
        for p in problems:
            print(p)

    except Exception as e:
        print(f"Error in best_idea generator test: {e}")
