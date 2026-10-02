"""
Refreshes every listing's free float and every company's float factor (the investable share of
its market cap per the Vanguard index funds — information and a market-cap check; benchmark
weights use the whole company cap; modules/ticker/free_float.py), as the cron does on the
generation day before the company builder, and reports: the large-cap market caps below the index funds'
float cap (too low — worth checking), the latest screened companies without a factor and the lowest factors,
and the listings found with no equity float (notes / preferreds the company builder leaves out).

Report: .output/float_factors_report.md

Usage:
    python scripts/current_float_factors.py --dev
    python scripts/current_float_factors.py --prod
"""
import atexit
import os
from datetime import date, datetime
from modules.object.exit import cleanup
from modules.object import ticker, screener_company
from modules.cron.benchmark_generator import current_companies
from modules.ticker import free_float

atexit.register(cleanup)

REPORT_PATH = os.path.join(os.path.dirname(__file__), '..', '.output', 'float_factors_report.md')

if __name__ == '__main__':
    stats = free_float.refresh()
    lines = ["# Float Factors Report", "", f"Generated: {datetime.now():%Y-%m-%d %H:%M:%S}", "", free_float.summary(stats), ""]
    lines += [f"Index fund scale (float cap per $ held): " + ", ".join(f"{f} {k:,.1f}" for f, k in stats.scale.items()), ""]
    lines += [f"## Market caps below the index funds' float cap: {len(stats.cap_checks)}", "",
              f"An index fund can't hold more of a company than the whole company: a float cap {free_float.CAP_CHECK_FACTOR:g}x "
              "the company cap or more means the company cap looks too low (largest first).", ""]
    lines += [f"- {x}" for x in stats.cap_checks] or ["None."]
    lines += [""]

    screen_date = screener_company.fetch_latest_date(up_to=date.today())
    if screen_date:
        companies = current_companies(screener_company.fetch_for_date(screen_date))
        by_id = {t.id: t for t in ticker.fetch_by_ids([cid for cid, _r, _mc in companies])}
        rows = [(by_id[cid], region, mc) for cid, region, mc in companies if cid in by_id]
        missing = [(t, region, mc) for t, region, mc in rows if t.float_factor is None]
        lines += [f"## Screened companies {screen_date}: {len(rows)} companies, {len(rows) - len(missing)} with a float factor", ""]
        lines += ["### Without a float factor (weighted in full)", ""]
        lines += [f"- {t.symbol}:{t.exchange} {t.name} ({region}, ${mc / 1e9:,.1f}B)" for t, region, mc in sorted(missing, key=lambda r: -r[2])] or ["None."]
        low = sorted((r for r in rows if r[0].float_factor is not None), key=lambda r: r[0].float_factor)[:40]
        lines += ["", "### Lowest float factors (information — weights use the whole cap)", ""]
        lines += [f"- {t.float_factor:.2f} {t.symbol}:{t.exchange} {t.name} ({region}, ${mc / 1e9:,.1f}B -> ${mc * t.float_factor / 1e9:,.1f}B investable)"
                  for t, region, mc in low]
    lines += ["", f"## Listings with no equity float: {len(stats.zero_float)}", ""] + ([f"- {x}" for x in stats.zero_float] or ["None."])

    report = "\n".join(lines)
    print(report)
    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, 'w', encoding='utf-8') as f:
        f.write(report)
    print(f"\nReport written to: {REPORT_PATH}")
