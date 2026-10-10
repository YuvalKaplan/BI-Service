"""
Read-only report of the provider ETF holding lines left unresolved - the "stock line(s)
unresolved" of the cron email (modules/cron/etf_downloader.py): in each active or pending ETF's
latest holdings, the lines with no ticker, a positive weight, that etf_profile.is_stock_line
takes for a stock (not a fund FMP knows, cash, a currency, a derivative, a note...).

Lines are grouped across ETFs (FMP's symbol / name / ISIN / CUSIP), heaviest first, each with the
tickers of ours it points at by FMP symbol, ISIN or CUSIP, valid or not, and why an invalid one
is - a stock we have but marked invalid shows up here. The email shows only the ETFs with the
most unresolved weight; this is the full list behind it.

Writes .output/unresolved_lines.csv.

Usage:
    python scripts/debug_unresolved_lines.py --dev
    python scripts/debug_unresolved_lines.py --prod
"""
import atexit
import csv
import os
from collections import defaultdict
from modules.core.db import db_pool_instance
from modules.object import ticker
from modules.object.exit import cleanup
from modules.sec import etf_profile
from modules.ticker import index_funds
from modules.ticker.resolver import TickerResolver

atexit.register(cleanup)

OUTPUT = os.path.join('.output', 'unresolved_lines.csv')
TOP = 40   # lines printed (all go to the CSV)

if __name__ == '__main__':
    with db_pool_instance.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                WITH latest AS (SELECT provider_etf_id, MAX(holding_date) AS holding_date
                                FROM provider_etf_holding GROUP BY provider_etf_id)
                SELECT pe.ticker, h.symbol, h.name, h.isin, h.cusip, h.weight, h.shares, h.market_value
                FROM provider_etf_holding h
                JOIN latest l ON l.provider_etf_id = h.provider_etf_id AND l.holding_date = h.holding_date
                JOIN provider_etf pe ON pe.id = h.provider_etf_id
                WHERE pe.status IN ('active', 'pending') AND h.ticker_id IS NULL AND h.weight > 0;
            """)
            rows = cur.fetchall()

    listings = index_funds.our_listings()
    resolver = TickerResolver(TickerResolver.POPULATE_TICKER)
    ours: dict[str, list] = defaultdict(list)   # FMP full symbol / ISIN / CUSIP -> our tickers, valid or not
    for t in ticker.fetch_all():
        for key in {resolver.get_full_symbol(t), t.isin, t.cusip}:
            if key:
                ours[key].append(t)

    groups = defaultdict(list)
    per_etf: dict[str, float] = defaultdict(float)
    for etf, symbol, name, isin, cusip, weight, shares, market_value in rows:
        line = {'asset': symbol, 'name': name, 'isin': isin, 'securityCusip': cusip}
        if not etf_profile.is_stock_line(line, None, listings):
            continue
        groups[(symbol, name, isin, cusip)].append((etf, weight, shares, market_value))
        per_etf[etf] += weight

    ordered = sorted(groups.items(), key=lambda kv: -sum(x[1] for x in kv[1]))
    os.makedirs('.output', exist_ok=True)
    with open(OUTPUT, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['symbol', 'name', 'isin', 'cusip', 'etfs', 'total_weight', 'implied_price', 'our_tickers', 'etf_list'])
        for (symbol, name, isin, cusip), lines in ordered:
            matches = {t.id: t for k in (symbol, isin, cusip) if k for t in ours.get(k, [])}
            price = next((mv / sh for *_, sh, mv in lines if sh and mv), None)
            w.writerow([
                symbol, name, isin, cusip, len(lines), round(sum(x[1] for x in lines), 6),
                round(price, 4) if price else None,
                ' '.join(f"{t.symbol}/{t.exchange}" + (f" [{t.invalid[:40]}]" if t.invalid else '') for t in matches.values()),
                ' '.join(f"{e}({wt:.2%})" for e, wt, *_ in sorted(lines, key=lambda x: -x[1])),
            ])

    print(f"{sum(len(v) for v in groups.values())} unresolved stock line(s), {len(groups)} distinct, in {len(per_etf)} ETF(s)\n")
    print("Most unresolved weight by ETF:")
    for etf, wt in sorted(per_etf.items(), key=lambda kv: -kv[1])[:15]:
        print(f"  {etf:8s} {wt:.1%}")
    print(f"\nHeaviest lines (all {len(groups)} in {OUTPUT}):")
    for (symbol, name, isin, cusip), lines in ordered[:TOP]:
        print(f"  {(symbol or '-'):14.14s} {(name or '-'):42.42s} {(isin or cusip or '-'):13s} "
              f"{len(lines):2d} ETF(s) {sum(x[1] for x in lines):6.2%}")
