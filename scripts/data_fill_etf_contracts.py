"""
Marks provider_etf.contracts_found from every stored holdings date: the first date an ETF's
holdings show an option or an equity-linked note (etf_selection.is_contract_line). The holdings
download and the SEC profiles mark new ones as they see them; this fills the history once (after
the column was added) and can be re-run at any time - an earlier date replaces a later one,
nothing is cleared. Then the selection runs, so marked ETFs become inactive.

    python scripts/data_fill_etf_contracts.py [--dry-run] [--prod]
"""
import atexit
import sys
from datetime import date
from modules.object import provider_etf, provider_etf_holding
from modules.object.exit import cleanup
from modules.sec import etf_selection

atexit.register(cleanup)


if __name__ == '__main__':
    dry_run = '--dry-run' in sys.argv
    found: dict[int, date] = {}
    lines: dict[int, int] = {}
    for etf_id, day, symbol, name in provider_etf_holding.fetch_numbered_lines():
        if etf_selection.is_contract_line(symbol, name):
            found[etf_id] = min(found.get(etf_id, day), day)
            lines[etf_id] = lines.get(etf_id, 0) + 1
    etfs = {e.id: e for e in provider_etf.fetch_all()}
    print(f"{len(found)} ETF(s) whose stored holdings show options / equity-linked notes:")
    for etf_id, day in sorted(found.items(), key=lambda kv: (etfs[kv[0]].ticker or '') if kv[0] in etfs else ''):
        e = etfs.get(etf_id)
        new = '' if e is not None and e.contracts_found is not None else '  (new)'
        print(f"  {e.ticker if e else etf_id:8} {e.status if e else '':8} first {day}, {lines[etf_id]} line(s){new}")
    if dry_run:
        print("Dry run - nothing written.")
    else:
        provider_etf.mark_contracts(found)
        etf_selection.run()     # logs its summary
