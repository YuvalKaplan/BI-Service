"""
The SEC active ETF list, as the Sunday cron runs it:
  1. Reads the Form N-CEN filings on EDGAR not read yet (the last five quarters) into the list of
     actively managed ETFs (sec_active_etf - modules/sec/ncen.py). The first run reads about
     4,000 filings (15-25 minutes); later runs only the new ones. --reload reads every filing in
     the window again (the result is the same).
  2. Profiles the listed funds due for it with FMP (modules/sec/etf_profile.py): each one's
     strategy (sec_etf_classification), and the actively managed equity funds - with region, cap
     size, value / growth and sectors - placed in the test provider tables, disabled until an admin
     approves them, their holdings stored once. --refresh-all profiles every listed fund again.

Writes .output/sec_active_etfs.csv: every current fund with its strategy and, for the equity
funds, their test profile - next to provider_etf's cap / style / region where we already track the
fund - followed by the provider_etf funds not on the list (index funds by their own filings, or
not filed yet).

Needs SECRET_SEC_USER_AGENT (e.g. "BI-Service admin@example.com") in the environment.

Usage:
    python scripts/current_sec_active_etfs.py --dev
    python scripts/current_sec_active_etfs.py --dev --reload --refresh-all
"""
import atexit
import csv
import os
import sys
from modules.object.exit import cleanup
from modules.object import sec_active_etf, sec_etf_classification, test_provider_etf
from modules.sec import etf_profile, ncen

atexit.register(cleanup)

OUTPUT_PATH = os.path.join('.output', 'sec_active_etfs.csv')
SEC_COLUMNS = ['series_id', 'ticker', 'fund_name', 'registrant_name', 'adviser_name', 'is_etmf', 'is_fund_of_funds',
               'is_multiple_inverse', 'net_assets', 'report_period', 'filing_date']
PROFILE_COLUMNS = ['region', 'cap_type', 'style_type', 'aum', 'stock_weight', 'us_weight', 'avg_float_cap',
                   'large_weight', 'mid_weight', 'small_weight', 'value_weight', 'growth_weight', 'top_sector', 'website']

if __name__ == '__main__':
    stats = ncen.run(reload='--reload' in sys.argv)
    profiles = etf_profile.run(refresh_all='--refresh-all' in sys.argv)
    print(ncen.summary(stats))
    print(etf_profile.summary(profiles))

    current = sec_active_etf.fetch_current(ncen.filed_since())
    classes = sec_etf_classification.fetch_all()
    tests = {e.sec_series_id: e for e in test_provider_etf.fetch_all() if e.sec_series_id}
    tracked = sec_active_etf.fetch_tracked_tickers()  # ticker -> (provider_etf id, cap_type, style_type, region)
    listed = {(etf.ticker or '').upper() for etf in current}

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(SEC_COLUMNS + ['strategy', 'asset_class', 'equity_weight', 'fmp_error', 'test_provider_etf_id']
                        + PROFILE_COLUMNS + ['provider_etf_id', 'provider_cap_type', 'provider_style_type', 'provider_region'])
        for etf in current:
            c = classes.get(etf.series_id)
            t = tests.get(etf.series_id)
            pe = tracked.get((etf.ticker or '').upper())
            writer.writerow(
                [getattr(etf, col) for col in SEC_COLUMNS]
                + [c.strategy if c else '', c.asset_class if c else '', c.equity_weight if c else '', c.fmp_error if c else '',
                   t.id if t else '']
                + [getattr(t, col) if t else '' for col in PROFILE_COLUMNS]
                + (list(pe) if pe else ['', '', '', '']))
        writer.writerow([])
        writer.writerow(['provider_etf funds not on the list (index funds by their own filings, or not filed yet)'])
        writer.writerow(['ticker', 'provider_etf_id', 'cap_type', 'style_type', 'region'])
        for ticker, pe in sorted(tracked.items()):
            if ticker not in listed:
                writer.writerow([ticker, *pe])

    print(f"{len(current)} current active ETFs written to {OUTPUT_PATH}")
