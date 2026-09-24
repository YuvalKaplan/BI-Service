"""
Rebuilds ticker_value history since inception (2026-01-01) directly from FMP's historical
price/market-cap endpoints, replacing whatever is currently stored for each ticker.

This is NOT a report-only audit - it unconditionally deletes and rewrites each ticker's
ticker_value rows in the resynced range, so already-corrupted data (e.g. a bad snapshot picked
up by the live pipeline's company-profile/screener calls) gets fixed rather than merely flagged.
A ticker is only touched if FMP's historical endpoints return usable data for it; if the fetch
fails, that ticker's existing data is left untouched (never cleared without a replacement).

Usage:
    python scripts/data_fill_ticker_value_refresh.py                    # all valid tickers, asks to confirm
    python scripts/data_fill_ticker_value_refresh.py --yes               # skip the confirmation prompt
    python scripts/data_fill_ticker_value_refresh.py --dry-run           # run everything, always roll back
    python scripts/data_fill_ticker_value_refresh.py --symbol AVGO       # limit to one ticker, for testing

Writes a report (summary counts, tickers with mismatches, skipped tickers) to
.downloads/data_fill_ticker_value_refresh_report.md.
"""
import argparse
import atexit
import os
from datetime import date, datetime
from concurrent.futures import ThreadPoolExecutor

from modules.object.exit import cleanup
from modules.object import ticker
from modules.object.ticker import Ticker
from modules.object.ticker_value import TickerValue, fetch_values_for_ticker, replace_range
from modules.ticker.resolver import TickerResolver
from modules.ticker import pricing
from modules.ticker import util as tu

atexit.register(cleanup)

INCEPTION_DATE = date(2026, 1, 1)  # matches scripts/data_fill_ticker_value_gaps.py::FILL_START_DATE
REPORT_PATH = os.path.join(os.path.dirname(__file__), '..', '.downloads', 'data_fill_ticker_value_refresh_report.md')


def resync_ticker(t: Ticker, resolver: TickerResolver, end_date: date, dry_run: bool):
    """Returns (symbol, rows_written, mismatches, error)."""
    assert t.id is not None

    full_symbol = resolver.get_full_symbol(t)
    fetched = pricing.fetch_price_and_market_cap_history(
        full_symbol, INCEPTION_DATE, end_date, currency=tu.currency_for_exchange(t.exchange),
    )
    if isinstance(fetched, str):
        return full_symbol, 0, [], fetched

    old = fetch_values_for_ticker(t.id, INCEPTION_DATE, end_date)
    mismatches = pricing.compare_overlap(t.id, fetched, old)

    items = [
        TickerValue(ticker_id=t.id, value_date=d, stock_price=price, market_cap=market_cap)
        for d, (price, market_cap) in fetched.items()
    ]
    replace_range(t.id, INCEPTION_DATE, end_date, items, dry_run=dry_run)

    return full_symbol, len(items), mismatches, None


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--symbol', help="Limit to a single ticker symbol, for testing.")
    parser.add_argument('--dry-run', action='store_true', help="Run the whole resync but roll back every write.")
    parser.add_argument('--yes', action='store_true', help="Skip the confirmation prompt.")
    args, _unknown = parser.parse_known_args()  # --prod/--dev are read directly from sys.argv by modules/core/db.py

    if args.symbol:
        # A symbol can exist on more than one exchange (e.g. a cross-listing) - resync every
        # matching row rather than arbitrarily picking one.
        tickers = ticker.fetch_by_symbols([args.symbol])
        if not tickers:
            print(f"No ticker found for symbol '{args.symbol}'.")
            return
        if len(tickers) > 1:
            print(f"Note: {len(tickers)} tickers match symbol '{args.symbol}': "
                  + ", ".join(f"id={t.id} exchange={t.exchange}" for t in tickers))
    else:
        tickers = ticker.fetch_all_valid()

    end_date = date.today()
    print(
        f"About to resync ticker_value for {len(tickers)} ticker(s) from {INCEPTION_DATE} to {end_date}"
        + (" (DRY RUN - nothing will be committed)." if args.dry_run else ".")
    )

    if not args.dry_run and not args.yes:
        if input("Continue? [y/N] ").strip().lower() != 'y':
            print("Aborted.")
            return

    resolver = TickerResolver(TickerResolver.POPULATE_TICKER)

    processed = 0
    skipped = 0
    total_rows = 0
    tickers_with_diffs = 0
    mismatch_lines: list[str] = []
    skipped_lines: list[str] = []

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(resync_ticker, t, resolver, end_date, args.dry_run): t for t in tickers}
        for i, future in enumerate(futures, start=1):
            t = futures[future]
            try:
                symbol, rows, mismatches, error = future.result()
            except Exception as e:
                print(f"[{i}/{len(tickers)}] {t.symbol}: error - {e}")
                skipped += 1
                skipped_lines.append(f"- {t.symbol}: error - {e}")
                continue

            if error:
                print(f"[{i}/{len(tickers)}] {symbol}: skipped - {error}")
                skipped += 1
                skipped_lines.append(f"- {symbol}: {error}")
                continue

            processed += 1
            total_rows += rows
            if mismatches:
                tickers_with_diffs += 1
                worst = max(mismatches, key=lambda m: m.pct_diff)
                print(
                    f"[{i}/{len(tickers)}] {symbol}: {rows} rows replaced, {len(mismatches)} differed from "
                    f"prior data by >tolerance (worst: {worst.value_date}, {worst.field} "
                    f"{worst.stored_value:.4g} -> {worst.fetched_value:.4g})"
                )
                mismatch_lines.append(
                    f"- {symbol}: {rows} rows replaced, {len(mismatches)} differed from prior data by "
                    f">tolerance (worst: {worst.value_date}, {worst.field} "
                    f"{worst.stored_value:.4g} -> {worst.fetched_value:.4g})"
                )
            else:
                print(f"[{i}/{len(tickers)}] {symbol}: {rows} rows replaced")

    print(
        f"\nDone. Processed {processed} ticker(s), skipped {skipped} (no data available), "
        f"{total_rows} total rows replaced, {tickers_with_diffs} ticker(s) had at least one meaningful diff."
    )

    report_lines = [
        "# Ticker Value Refresh Report",
        "",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"Range: {INCEPTION_DATE} to {end_date}" + (" (DRY RUN)" if args.dry_run else ""),
        f"Scope: {'symbol=' + args.symbol if args.symbol else 'all valid tickers'}",
        "",
        "## Summary",
        "",
        f"Processed: {processed} | Skipped: {skipped} | Rows written: {total_rows} | "
        f"Tickers with mismatches: {tickers_with_diffs}",
        "",
        f"## Tickers with mismatches ({len(mismatch_lines)}) — worth reviewing",
        "",
    ] + (mismatch_lines or ["None."]) + [
        "",
        f"## Skipped tickers ({len(skipped_lines)}) — no usable data",
        "",
    ] + (skipped_lines or ["None."])

    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, 'w', encoding='utf-8') as f:
        f.write("\n".join(report_lines))
    print(f"Report written to: {REPORT_PATH}")


if __name__ == '__main__':
    main()
