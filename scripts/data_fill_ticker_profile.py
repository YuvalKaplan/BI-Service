import atexit
from modules.object.exit import cleanup
from modules.ticker import master

atexit.register(cleanup)


if __name__ == "__main__":
    include_invalid = input("Also retry tickers previously marked invalid? [y/N] ").strip().lower() == 'y'

    total, updated, marked_invalid = master.refresh_ticker_profiles(include_invalid=include_invalid)

    print(f"\nDone. Checked: {total} | Updated: {updated} | Marked invalid: {marked_invalid}")
