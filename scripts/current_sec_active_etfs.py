"""
The SEC active ETF list, as the cron runs it on the weekly day:
  1. Reads the Form N-CEN filings on EDGAR not read yet (the last five quarters) into the list of
     actively managed ETFs (sec_active_etf - modules/sec/ncen.py). The first run reads about
     4,000 filings (15-25 minutes); later runs only the new ones. --reload reads every filing in
     the window again (the result is the same).
  2. Profiles the listed funds due for it with FMP (modules/sec/etf_profile.py): each one's
     strategy (sec_etf_classification), and the actively managed equity funds - with region, cap
     size, value / growth, sectors, countries and emerging share - placed in provider_etf.
     --refresh-all profiles every listed fund again (needed once after migration 18).
  3. Applies the selection rules (modules/sec/etf_selection.py): each provider ETF's status.

Writes .output/sec_active_etfs.csv: every current fund with its strategy and, for the equity
funds, their profile, status, latest holdings date and the rules they fail - next to the old
hand-picked old_provider_etf's cap / style / region where it had the fund - followed by the old
enabled funds and what became of them (to compare the rules with the hand-picked list).

Needs SECRET_SEC_USER_AGENT (e.g. "BI-Service admin@example.com") in the environment.

Usage:
    python scripts/current_sec_active_etfs.py --dev
    python scripts/current_sec_active_etfs.py --dev --reload --refresh-all
"""
import atexit
import csv
import os
import sys
from collections import Counter
from modules.object.exit import cleanup
from modules.object import sec_active_etf, sec_etf_classification
from modules.sec import etf_profile, etf_selection, ncen

atexit.register(cleanup)

OUTPUT_PATH = os.path.join('.output', 'sec_active_etfs.csv')
SEC_COLUMNS = ['series_id', 'ticker', 'fund_name', 'registrant_name', 'adviser_name', 'is_etmf', 'is_fund_of_funds',
               'is_multiple_inverse', 'net_assets', 'report_period', 'filing_date']
PROFILE_COLUMNS = ['region', 'cap_type', 'style_type', 'aum', 'stock_holdings', 'stock_weight', 'us_weight',
                   'avg_float_cap', 'large_weight', 'mid_weight', 'small_weight', 'value_weight', 'growth_weight',
                   'top_sector', 'top_sector_weight', 'top_country', 'top_country_weight', 'emerging_weight', 'website']
OLD_COLUMNS = ['old_provider_etf_id', 'old_enabled', 'old_cap_type', 'old_style_type', 'old_region']

if __name__ == '__main__':
    stats = ncen.run(reload='--reload' in sys.argv)
    profiles = etf_profile.run(refresh_all='--refresh-all' in sys.argv)
    print(ncen.summary(stats))
    print(etf_profile.summary(profiles))
    if profiles.selection is not None:
        print("\n" + etf_selection.summary(profiles.selection))

    current = sec_active_etf.fetch_current(ncen.filed_since())
    classes = sec_etf_classification.fetch_all()
    evaluated = {e.etf.sec_series_id: e for e in etf_selection.evaluate() if e.etf.sec_series_id}
    by_ticker = {(e.etf.ticker or '').upper(): e for e in evaluated.values() if e.etf.ticker}
    old = sec_active_etf.fetch_old_tracked_tickers()  # ticker -> (old id, enabled, cap_type, style_type, region)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(SEC_COLUMNS + ['strategy', 'asset_class', 'equity_weight', 'fmp_error', 'provider_etf_id', 'status',
                                       'latest_holdings', 'failed_rules'] + PROFILE_COLUMNS + OLD_COLUMNS)
        for etf in current:
            c = classes.get(etf.series_id)
            e = evaluated.get(etf.series_id)
            pe = e.etf if e else None
            o = old.get((etf.ticker or '').upper())
            writer.writerow(
                [getattr(etf, col) for col in SEC_COLUMNS]
                + [c.strategy if c else '', c.asset_class if c else '', c.equity_weight if c else '', c.fmp_error if c else '']
                + ([pe.id, e.status, e.latest or '', ' '.join(e.failed)] if e and pe else ['', '', '', ''])
                + [getattr(pe, col) if pe else '' for col in PROFILE_COLUMNS]
                + (list(o) if o else [''] * len(OLD_COLUMNS)))

        writer.writerow([])
        writer.writerow(['old enabled provider ETFs (hand-picked) and what the rules make of them'])
        writer.writerow(['ticker', 'old_provider_etf_id', 'old_cap_type', 'old_style_type', 'old_region', 'status', 'failed_rules'])
        outcome: Counter = Counter()
        for ticker, (old_id, enabled, cap, style, region) in sorted(old.items()):
            if not enabled:
                continue
            e = by_ticker.get(ticker)
            status = e.status if e else 'not in provider_etf'
            outcome[status] += 1
            writer.writerow([ticker, old_id, cap, style, region, status, ' '.join(e.failed) if e else ''])

    print(f"{len(current)} current active ETFs written to {OUTPUT_PATH}")
    print(f"Old enabled provider ETFs with a ticker: {sum(outcome.values())} - " + ", ".join(f"{k} {v}" for k, v in outcome.most_common()))
