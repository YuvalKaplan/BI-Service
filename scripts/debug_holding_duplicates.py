"""
Read-only report of provider_etf_holding lines that share a ticker within one ETF and holding
date, classified the same way the best ideas generator treats them (see
provider_etf_holding.aggregate_holdings):
  - consistent:   every line implies the same price -> lots of one security, summed
  - inconsistent: implied prices disagree           -> likely mis-resolved tickers, quarantined

Use it to confirm ticker-resolution fixes (new holding dates should show few or no
inconsistent groups) and to spot providers whose mapping needs an ISIN/CUSIP column.

Writes the report to .output/holding_duplicates_report.md.

Usage:
    python scripts/debug_holding_duplicates.py --dev
    python scripts/debug_holding_duplicates.py --prod
"""
import atexit
import os
from collections import defaultdict
from datetime import datetime
from psycopg.rows import class_row
from modules.core.db import db_pool_instance
from modules.object.exit import cleanup
from modules.object.provider_etf_holding import ProviderEtfHolding, aggregate_holdings

atexit.register(cleanup)

REPORT_PATH = os.path.join(os.path.dirname(__file__), '..', '.output', 'holding_duplicates_report.md')

if __name__ == '__main__':
    try:
        days_back = 60  # <-- edit before each run (None = all history)

        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(ProviderEtfHolding)) as cur:
                cur.execute(
                    f"""
                    SELECT peh.*
                    FROM provider_etf_holding peh
                    JOIN (
                        SELECT provider_etf_id, holding_date, ticker_id
                        FROM provider_etf_holding
                        WHERE ticker_id IS NOT NULL
                          {"AND holding_date > NOW() - (%s * INTERVAL '1 day')" if days_back else ""}
                        GROUP BY 1, 2, 3
                        HAVING COUNT(*) > 1
                    ) d USING (provider_etf_id, holding_date, ticker_id)
                    ORDER BY peh.provider_etf_id, peh.holding_date, peh.id
                    """,
                    (days_back,) if days_back else None,
                )
                lines = cur.fetchall()
            with conn.cursor() as cur:
                cur.execute("SELECT id, name FROM provider_etf")
                etf_names = dict(cur.fetchall())
                cur.execute("SELECT id, symbol, exchange FROM ticker WHERE id = ANY(%s)", (list({h.ticker_id for h in lines}),))
                symbols = {r[0]: f"{r[1]} ({r[2]})" for r in cur.fetchall()}

        by_etf_date: dict[tuple[int, object], list[ProviderEtfHolding]] = defaultdict(list)
        for h in lines:
            by_etf_date[(h.provider_etf_id, h.holding_date)].append(h)

        summary: dict[int, list[int]] = defaultdict(lambda: [0, 0])  # etf_id -> [consistent, inconsistent]
        details: list[str] = []
        for (etf_id, holding_date), group in by_etf_date.items():
            holdings, quarantined = aggregate_holdings(group)
            summary[etf_id][0] += len(holdings)
            summary[etf_id][1] += len(quarantined)
            for q in quarantined:
                prices = ", ".join(f"{p:,.2f}" if p else "?" for p in q.implied_prices)
                details.append(f"| {etf_id} | {holding_date:%Y-%m-%d} | {symbols.get(q.ticker_id, q.ticker_id)} | {len(q.lines)} | {prices} |")

        total_consistent = sum(v[0] for v in summary.values())
        total_inconsistent = sum(v[1] for v in summary.values())
        report_lines = [
            "# Holding Duplicates Report",
            "",
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"Window: {'last ' + str(days_back) + ' days' if days_back else 'all history'}",
            "",
            f"Duplicate groups: {total_consistent + total_inconsistent} "
            f"(consistent / summed: {total_consistent}, inconsistent / quarantined: {total_inconsistent})",
            "",
            "## Per ETF",
            "",
            "| ETF id | Name | Consistent | Inconsistent |",
            "|---|---|---|---|",
        ] + [
            f"| {etf_id} | {etf_names.get(etf_id, '')} | {c} | {i} |"
            for etf_id, (c, i) in sorted(summary.items(), key=lambda kv: -sum(kv[1]))
        ] + [
            "",
            "## Quarantined groups",
            "",
            "| ETF id | Holding date | Ticker | Lines | Implied prices |",
            "|---|---|---|---|---|",
        ] + details

        report = "\n".join(report_lines)
        print("\n".join(report_lines[:12 + len(summary)]))
        os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
        with open(REPORT_PATH, 'w', encoding='utf-8') as f:
            f.write(report)
        print(f"\nReport written to: {REPORT_PATH}")

    except Exception as e:
        print(f"Error in holding duplicates report: {e}")
