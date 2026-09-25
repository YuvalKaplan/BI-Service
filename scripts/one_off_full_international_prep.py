"""
One-off script: discovers the full international ticker universe FIRST (a multi-exchange
screener sweep), so the ticker_value refresh step right after it covers every newly-discovered
ticker too — not just the ones already in the DB before this run — then runs the full sim data
prep flow.

This exists to close a one-run gap in the normal ongoing order (data_fill_ticker_value_refresh.py,
then sim_prep_data.py): sim_prep_data.py's own ticker discovery happens INSIDE
benchmark_generator.run(), which runs after master-ticker sync, so brand-new international
tickers discovered that run wouldn't get their deep history backfilled until a LATER refresh
run. This script does discovery before the refresh instead, in one pass:

  1. Discover  - sweep the multi-exchange screener and register every ticker it finds
                 (modules.cron.benchmark_generator._fetch_all_screener_results/_resolve_tickers).
                 No benchmark snapshot is built yet.
  2. Refresh   - rebuild ticker_value history (since inception, currency-converted) for every
                 valid ticker now in the table, including the ones just discovered in step 1
                 (reuses scripts/data_fill_ticker_value_refresh.py's resync_ticker, minus its
                 interactive confirmation prompt — this script's own run is the confirmation).
  3. Sim prep  - ticker profile refresh, master ticker sync/accumulated-cap refresh, and the
                 historical benchmark_holding backfill (the same three steps
                 scripts/sim_prep_data.py runs).

Meant to be run once (or occasionally), not on a schedule — step 3 repeats some of the same
screener/resolution work step 1 already did, which is an acceptable trade-off for a one-off
comprehensive run rather than the normal two-pass (refresh, sim prep, refresh again) cadence.

Writes a combined report to .downloads/one_off_full_international_prep_report.md.

Usage:
    python scripts/one_off_full_international_prep.py --dev
    python scripts/one_off_full_international_prep.py --prod
"""
import atexit
import os
from datetime import date, datetime
from concurrent.futures import ThreadPoolExecutor

from modules.object.exit import cleanup
from modules.object import ticker
from modules.ticker.resolver import TickerResolver
from modules.ticker import master
from modules.cron.benchmark_generator import _fetch_all_screener_results, _resolve_tickers
from modules.sim import benchmark_generator
from data_fill_ticker_value_refresh import resync_ticker, INCEPTION_DATE

atexit.register(cleanup)

REPORT_PATH = os.path.join(os.path.dirname(__file__), '..', '.downloads', 'one_off_full_international_prep_report.md')


def discover_tickers() -> int:
    """Stage 1+2 of the normal benchmark_generator flow, run standalone: sweeps the
    multi-exchange screener and upserts every ticker it finds, without building any benchmark
    snapshot yet. Registers brand-new international tickers so the refresh step right after
    this one covers them too. require_validated_price=False for the same reason it's used in
    the sim path: a withheld/failed validation on today's date shouldn't block a ticker from
    being registered here."""
    screener_results = _fetch_all_screener_results()
    print(f"Fetched {len(screener_results)} companies from the multi-exchange screener sweep.")
    resolved = _resolve_tickers(screener_results, value_date=date.today(), require_validated_price=False)
    print(f"Resolved/registered {len(resolved)} tickers.")
    return len(resolved)


def refresh_ticker_value_history() -> tuple[int, int, int]:
    """Same work as scripts/data_fill_ticker_value_refresh.py's main(), minus the CLI flags
    and confirmation prompt — always the full valid-ticker set, always for real."""
    tickers = ticker.fetch_all_valid()
    end_date = date.today()
    print(f"Resyncing ticker_value for {len(tickers)} ticker(s) from {INCEPTION_DATE} to {end_date}.")

    resolver = TickerResolver(TickerResolver.POPULATE_TICKER)
    processed = 0
    skipped = 0
    total_rows = 0

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(resync_ticker, t, resolver, end_date, False): t for t in tickers}
        for i, future in enumerate(futures, start=1):
            t = futures[future]
            try:
                _symbol, rows, _mismatches, error = future.result()
            except Exception as e:
                print(f"[{i}/{len(tickers)}] {t.symbol}: error - {e}")
                skipped += 1
                continue
            if error:
                skipped += 1
                continue
            processed += 1
            total_rows += rows
            if i % 250 == 0:
                print(f"  ...{i}/{len(tickers)} processed")

    print(f"Ticker value refresh done. Processed {processed}, skipped {skipped}, {total_rows} rows written.")
    return processed, skipped, total_rows


if __name__ == '__main__':
    try:
        inception_date = date(2026, 6, 1)  # <-- edit before each run, same as sim_prep_data.py

        report_sections = ["\n".join([
            "# One-Off Full International Prep Report",
            "",
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"Inception date: {inception_date}",
        ])]

        print("=== Step 1: Discover international tickers ===")
        discovered = discover_tickers()
        report_sections.append("\n".join([
            "", "## Step 1: Ticker Discovery", "",
            f"{discovered} tickers resolved/registered.",
        ]))

        print("\n=== Step 2: Refresh ticker_value history ===")
        processed, skipped, total_rows = refresh_ticker_value_history()
        report_sections.append("\n".join([
            "", "## Step 2: Ticker Value Refresh", "",
            f"Processed: {processed} | Skipped: {skipped} | Rows written: {total_rows}",
        ]))

        print("\n=== Step 3: Sim data prep ===")
        # No confirmation prompt: this one-off run always covers every ticker, including
        # ones previously marked invalid (a retry, in case the invalid reason no longer holds).
        total, updated, marked_invalid = master.refresh_ticker_profiles(include_invalid=True)
        print(f"Ticker profile refresh: {updated} updated, {marked_invalid} marked invalid, out of {total} checked.")
        report_sections.append("\n".join([
            "", "## Step 3a: Ticker Profile Refresh", "",
            f"Checked: {total} | Updated: {updated} | Marked invalid: {marked_invalid}",
        ]))

        masters_updated, unlinked, caps_updated = master.sync_masters_and_accumulated_caps()
        print(f"Master sync: {masters_updated} link(s), {unlinked} unlinked, {caps_updated} accumulated cap(s) refreshed.")
        groups_report = master.build_master_groups_report(masters_updated, caps_updated, unlinked)
        print(f"\n{groups_report}")
        report_sections.append(groups_report)

        benchmarks_created = benchmark_generator.run(inception_date)
        benchmarks_lines = [
            "", "## Step 3c: Benchmarks Created", "",
            f"Total snapshots: {len(benchmarks_created)}", "",
        ] + [
            f"- {name} — {holding_date}: {num_holdings} holdings"
            for name, holding_date, num_holdings in benchmarks_created
        ]
        report_sections.append("\n".join(benchmarks_lines))

        report = "\n\n---\n".join(report_sections)
        os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
        with open(REPORT_PATH, 'w', encoding='utf-8') as f:
            f.write(report)
        print(f"\nReport written to: {REPORT_PATH}")

    except Exception as e:
        print(f"Error in one-off full international prep: {e}")
