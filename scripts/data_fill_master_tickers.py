import atexit
import os
from modules.object.exit import cleanup
from modules.ticker import master

atexit.register(cleanup)

REPORT_PATH = os.path.join(os.path.dirname(__file__), '..', '.downloads', 'master_tickers_report.md')

if __name__ == '__main__':
    masters_updated, caps_updated = master.sync_masters_and_accumulated_caps()
    print(f"Master sync: {masters_updated} link(s), {caps_updated} accumulated cap(s) refreshed")

    report = master.build_master_groups_report(masters_updated, caps_updated)
    print(f"\n{report}")

    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, 'w', encoding='utf-8') as f:
        f.write(report)
    print(f"\nReport written to: {REPORT_PATH}")
