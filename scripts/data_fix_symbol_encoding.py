"""
Repairs the tickers whose FMP symbol holds a character a URL query can't carry as is ('&': NSE's
M&M, M&MFIN, J&KBANK, ARE&M, GVT&D; BSE's M&MFIN, ARE&M; MEX's PE&OLES).

Until modules/core/api_stocks.py encoded its query values (api_stocks._q), every FMP call for such
a symbol answered for the part before the '&': M&M.NS got Macy's (M) profile - name, ISIN, CUSIP,
CIK, currency - and Macy's prices and market caps (stored as if in rupees); J&KBANK got Jacobs',
ARE&M Alexandria Real Estate's; GVT&D and PE&OLES were flagged invalid. Through the shared CIK /
ISIN the master sync grouped them with those companies - an NSE line became the master of Macy's,
making Macy's International, and Mahindra & Mahindra wasn't in the universe as itself.

For each such ticker (found by its FMP symbol):
  1. its master link, and those of the listings grouped under it (Macy's), are cleared, and so is
     what was derived from the wrong data (style, ESG, free float and float factor, turnover,
     verified shares, company cap, region, invalid flag);
  2. its FMP profile is fetched again (encoded) and stored - identifiers and name, sector,
     country, currency as the profile has them (a field the profile lacks is left empty, not
     kept: Indian listings have no CIK), invalid by the profile refresh's rules;
  3. its value history is rewritten from FMP (pricing.resync_value_history) - or cleared when FMP
     has none, since what's stored is another company's.
Then the master sync and company data refresh run (master.sync_masters_and_company_data), so the
companies are regrouped on the corrected identifiers. Style and ESG are filled again by the next
ticker maintenance (style: the daily run days; ESG: the weekly day, or at the next registration).

Run it only once the encoding fix is deployed, or the next profile refresh / valuation brings the
wrong data back.

Usage:
    python scripts/data_fix_symbol_encoding.py --dev --dry-run   # list the tickers and what FMP returns now, no writes
    python scripts/data_fix_symbol_encoding.py --dev             # asks to confirm
    python scripts/data_fix_symbol_encoding.py --prod --yes

Report: .output/data_fix_symbol_encoding_report.md
"""
import argparse
import atexit
import os
from datetime import date, datetime

from modules.object.exit import cleanup
from modules.object import ticker, ticker_value
from modules.object.ticker import Ticker
from modules.core import api_stocks
from modules.ticker import master, pricing
from modules.ticker import util as tu
from modules.ticker.resolver import TickerResolver

atexit.register(cleanup)

REPORT_PATH = os.path.join(os.path.dirname(__file__), '..', '.output', 'data_fix_symbol_encoding_report.md')
UNSAFE_CHARS = set('&#+=?% ')  # characters that break or change an unencoded query value


def invalid_reason(profile: dict) -> str | None:
    """The profile refresh's rules (modules/ticker/refresh.py::refresh_ticker_profiles)."""
    name = profile.get('companyName')
    is_active = profile.get('isActivelyTrading')
    if profile.get('exchange') == 'CRYPTO':
        return 'Crypto'
    if not name or tu.is_unwanted_names(name):
        return 'Fund or ETF'
    if is_active is not None and not is_active:
        return 'Not actively trading'
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--dry-run', action='store_true', help="List the tickers and the profile FMP returns now; write nothing.")
    parser.add_argument('--yes', action='store_true', help="Skip the confirmation prompt.")
    args, _unknown = parser.parse_known_args()  # --prod/--dev are read directly from sys.argv by modules/core/db.py

    resolver = TickerResolver(TickerResolver.POPULATE_TICKER)
    all_tickers = ticker.fetch_all()
    full_symbol = {t.id: resolver.get_full_symbol(t) for t in all_tickers}
    affected = [t for t in all_tickers if UNSAFE_CHARS & set(full_symbol[t.id] or t.symbol)]
    affected_ids = {t.id for t in affected}
    grouped_under = [t for t in all_tickers if t.master_ticker_id in affected_ids and t.id not in affected_ids]
    by_id = {t.id: t for t in all_tickers}

    lines = ["# Symbol encoding repair", "", f"Generated: {datetime.now():%Y-%m-%d %H:%M:%S}", "",
             f"## Tickers with a symbol FMP needs encoded: {len(affected)}", ""]
    profiles: dict[int, dict | str] = {}
    for t in affected:
        profiles[t.id] = api_stocks.get_stock_profile(full_symbol[t.id])
        p = profiles[t.id]
        now = (f"{p.get('companyName')} ({p.get('isin')}, {p.get('currency')}, {p.get('country')})"
               if isinstance(p, dict) else f"no profile: {p}")
        master_note = f", grouped under id {t.master_ticker_id} ({by_id[t.master_ticker_id].symbol})" if t.master_ticker_id in by_id else ""
        lines.append(f"- id {t.id} {full_symbol[t.id]}: stored {t.name} ({t.isin}, CIK {t.cik}, {t.currency}, {t.country})"
                     f"{master_note}{', invalid: ' + t.invalid if t.invalid else ''} -> FMP now: {now}")
    lines += ["", f"## Listings grouped under one of them (master link cleared): {len(grouped_under)}", ""]
    lines += [f"- id {t.id} {full_symbol[t.id]} {t.name} (region {t.region})" for t in grouped_under] or ["None."]
    print("\n".join(lines))

    if args.dry_run or not affected:
        print("\nDry run - nothing written." if args.dry_run else "\nNothing to repair.")
        return
    if not args.yes and input(f"\nRepair {len(affected)} ticker(s)? [y/N] ").strip().lower() != 'y':
        print("Aborted.")
        return

    # 1. Out of the wrong groups, and nothing kept that the wrong data produced.
    ticker.clear_master_ticker_bulk(sorted(affected_ids | {t.id for t in grouped_under}))
    ticker.clear_derived_data_bulk(sorted(affected_ids))

    # 2 + 3. The right profile, and the right history.
    lines += ["", "## Repaired", ""]
    for t in affected:
        p = profiles[t.id]
        if not isinstance(p, dict):
            ticker.update_invalid(t.id, "Profile lookup failed")
            ticker_value.replace_range(t.id, date(2000, 1, 1), date.today(), [])
            lines.append(f"- {full_symbol[t.id]}: no profile from FMP - marked invalid, stored values cleared")
            continue
        ticker.update(Ticker(
            id=t.id, symbol=t.symbol, exchange=t.exchange, source=t.source, type_from=None,
            isin=p.get('isin') or None, cusip=p.get('cusip') or None, cik=p.get('cik') or None,
            name=p.get('companyName') or t.name, industry=p.get('industry') or None, sector=p.get('sector') or None,
            country=p.get('country') or None, currency=p.get('currency') or None,
            is_actively_trading=bool(p['isActivelyTrading']) if p.get('isActivelyTrading') is not None else None,
            average_turnover=tu.profile_turnover(p),
        ))
        reason = invalid_reason(p)
        ticker.update_invalid(t.id, reason)
        currency = tu.listing_currency(t.exchange, p.get('currency'))
        quote_shares = p['marketCap'] / p['price'] if p.get('marketCap') and p.get('price') else None
        result = pricing.resync_value_history(t.id, full_symbol[t.id], currency, reference_shares=quote_shares)
        if isinstance(result, str):
            ticker_value.replace_range(t.id, date(2000, 1, 1), date.today(), [])
            result = f"no history from FMP ({result}) - stored values cleared"
        else:
            result = f"{result} values rewritten"
        lines.append(f"- {full_symbol[t.id]}: {p.get('companyName')} ({p.get('isin')}, {p.get('currency')})"
                     f"{', invalid: ' + reason if reason else ''} - {result}")

    # 4. Regroup on the corrected identifiers; company caps and regions again.
    masters_updated, unlinked, caps_updated = master.sync_masters_and_company_data()
    lines += ["", f"Master sync: {masters_updated} link(s), {unlinked} unlinked, {caps_updated} company cap(s) refreshed", ""]

    lines += ["## After", ""]
    for t in ticker.fetch_by_ids(sorted(affected_ids | {t.id for t in grouped_under})):
        m = by_id.get(t.master_ticker_id)
        lines.append(f"- id {t.id} {full_symbol[t.id]} {t.name}: region {t.region}, "
                     f"{'master ' + (m.symbol if m else str(t.master_ticker_id)) if t.master_ticker_id else 'its own master'}"
                     f"{', invalid: ' + t.invalid if t.invalid else ''}")

    report = "\n".join(lines)
    print("\n".join(lines[lines.index("## Repaired"):]))
    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, 'w', encoding='utf-8') as f:
        f.write(report)
    print(f"\nReport written to: {REPORT_PATH}")


if __name__ == '__main__':
    main()
