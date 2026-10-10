"""
Refreshes the FMP profile of every ticker not checked within a week (the daily cron's profile
refresh, modules/ticker/refresh.py), optionally retrying the invalid ones too.

--recheck-invalid checks again, however recently, only the tickers marked invalid by a rule that
changed (RECHECK_REASONS: the old name rule's 'Fund or ETF' - "Trust" / "Fund" in a company's
name - and FMP's isActivelyTrading flag, now confirmed by its daily prices). Every ticker whose
status changes is printed and written to .output/ticker_recheck.csv.

Usage:
    python scripts/data_fill_ticker_profile.py --dev
    python scripts/data_fill_ticker_profile.py --dev --recheck-invalid
"""
import atexit
import csv
import os
import sys
from modules.object import ticker
from modules.object.exit import cleanup
from modules.ticker import refresh

atexit.register(cleanup)

RECHECK_REASONS = ['Fund or ETF', 'Not actively trading']
OUTPUT = os.path.join('.output', 'ticker_recheck.csv')


def recheck_invalid() -> None:
    before = {t.id: t for t in ticker.fetch_invalid_for(RECHECK_REASONS)}
    print(f"Re-checking {len(before)} ticker(s) marked {' / '.join(RECHECK_REASONS)}...")
    total, updated, marked_invalid = refresh.refresh_ticker_profiles(recheck_reasons=RECHECK_REASONS)
    after = {t.id: t for t in ticker.fetch_by_ids(list(before))}
    changed = [(before[i], after[i]) for i in before if i in after and after[i].invalid != before[i].invalid]
    changed.sort(key=lambda ba: (ba[1].invalid or '', ba[0].invalid or '', ba[0].exchange or '', ba[0].symbol))

    os.makedirs('.output', exist_ok=True)
    with open(OUTPUT, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['id', 'symbol', 'exchange', 'name', 'was', 'now'])
        for b, a in changed:
            w.writerow([b.id, b.symbol, b.exchange, a.name, b.invalid, a.invalid or 'valid'])
    for b, a in changed:
        print(f"  {b.symbol:12s} {b.exchange or '':7s} {(a.name or '')[:45]:45s} {b.invalid} -> {a.invalid or 'valid'}")
    print(f"\nDone. Checked: {total} | Valid now: {updated} | Invalid: {marked_invalid} | "
          f"Status changed: {len(changed)} (written to {OUTPUT})")


if __name__ == "__main__":
    if '--recheck-invalid' in sys.argv:
        recheck_invalid()
    else:
        include_invalid = input("Also retry tickers previously marked invalid? [y/N] ").strip().lower() == 'y'
        total, updated, marked_invalid = refresh.refresh_ticker_profiles(include_invalid=include_invalid)
        print(f"\nDone. Checked: {total} | Updated: {updated} | Marked invalid: {marked_invalid}")
