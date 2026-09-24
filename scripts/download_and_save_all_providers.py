import atexit
import os
import shutil
from datetime import datetime

from modules.object.exit import cleanup
from modules.object import provider
from modules.parse.download import EtfStats, process_provider

atexit.register(cleanup)

DOWNLOADS_DIR = os.path.join(os.path.dirname(__file__), '..', '.downloads')


def _write_report(lines: list[str], path: str) -> None:
    with open(path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))


if __name__ == '__main__':
    if os.path.exists(DOWNLOADS_DIR):
        shutil.rmtree(DOWNLOADS_DIR)
    os.makedirs(DOWNLOADS_DIR)

    providers = provider.fetch_active_providers()
    print(f"Found {len(providers)} active providers.")

    report = [
        '# ETF Download Report',
        '',
        f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
        '',
        '---',
        '',
    ]

    grand_holdings = grand_tickers = grand_problems = 0

    for p in providers:
        try:
            print(f"Processing provider [{p.id}] - {p.name}...")
            provider_dir = os.path.join(DOWNLOADS_DIR, f"{p.id} - {p.name}")
            os.makedirs(provider_dir, exist_ok=True)

            etf_stats: list[EtfStats] = process_provider(p, save_dir=provider_dir)

            if not etf_stats:
                report += [f'## [{p.id}] {p.name}', '', '_No ETFs downloaded._', '']
                continue

            p_holdings = sum(s.holdings for s in etf_stats)
            p_tickers = sum(s.tickers for s in etf_stats)
            p_problems = sum(s.problems for s in etf_stats)
            p_pct = f"{100 * p_tickers / p_holdings:.1f}%" if p_holdings else "N/A"

            etf_rows = []
            problem_details = []
            for s in etf_stats:
                etf_id = s.etf_id or '—'
                region = s.etf_region or '—'
                if s.error:
                    etf_rows.append(f"| {etf_id} | {region} | {s.etf_name} | — | — | — | _error_ |")
                    problem_details.append((s.etf_name, [f"Error: {s.error}"]))
                else:
                    etf_rows.append(f"| {etf_id} | {region} | {s.etf_name} | {s.holdings} | {s.tickers} | {s.problems} | {s.match_pct:.1f}% |")
                    if s.problem_tickers:
                        problem_details.append((s.etf_name, s.problem_tickers))

            report += [
                f'## [{p.id}] {p.name}',
                '',
                '| ID | Region | ETF | Holdings | Tickers | Problems | Match Rate |',
                '|----|--------|-----|----------|---------|----------|------------|',
            ] + etf_rows + [
                '',
                f'**Provider Total:** {p_holdings} holdings | {p_tickers} tickers | {p_problems} problems | {p_pct} match rate',
                '',
            ]

            for etf_name, probs in problem_details:
                report += [
                    f'### {etf_name} — Unmatched ({len(probs)})',
                    '',
                    ', '.join(probs),
                    '',
                ]

            grand_holdings += p_holdings
            grand_tickers += p_tickers
            grand_problems += p_problems

        except Exception as e:
            print(f"  Error processing provider [{p.id}] - {p.name}: {e}")
            report += [f'## [{p.id}] {p.name}', '', f'**Error:** {e}', '']

    grand_pct = f"{100 * grand_tickers / grand_holdings:.1f}%" if grand_holdings else "N/A"
    report += [
        '---',
        '',
        '## Grand Total',
        '',
        '| Total Holdings | Tickers Upserted | Problem Holdings | Match Rate |',
        '|----------------|-----------------|-----------------|------------|',
        f'| {grand_holdings} | {grand_tickers} | {grand_problems} | {grand_pct} |',
        '',
    ]

    report_path = os.path.join(DOWNLOADS_DIR, 'report.md')
    _write_report(report, report_path)
    print(f"\nReport written to: {report_path}")
