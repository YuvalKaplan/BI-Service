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
    python scripts/data_fill_ticker_value_refresh.py --currency-mismatch # only listings FMP quotes in a currency
                                                                         # other than their exchange's (Compass on
                                                                         # LSE in USD, HK RMB counters) - stored with
                                                                         # the exchange's currency before 2026-09
    python scripts/data_fill_ticker_value_refresh.py --outliers          # only tickers whose stored market caps hold
                                                                         # an FMP glitch (pricing.market_cap_outliers:
                                                                         # e.g. Compass at 1/100 in late June 2026) -
                                                                         # rewritten with the glitches repaired from
                                                                         # the price, or dropped
    python scripts/data_fill_ticker_value_refresh.py --profile-check     # only tickers whose stored market cap
                                                                         # (any since 2026) is 2.5x+ off what their FMP
                                                                         # profile's share count gives (a history on
                                                                         # a wrong share count throughout, e.g. Uniper's
                                                                         # new XETRA line) - rewritten against the
                                                                         # profile's share count; one profile call each
    python scripts/data_fill_ticker_value_refresh.py --shares-check      # only tickers whose verified share count is set,
                                                                         # changed or cleared (refresh.verify_share_count:
                                                                         # stored history 1.15-2.5x off the profile's count,
                                                                         # confirmed by FMP's quarterly financials, e.g.
                                                                         # Rocket Companies) - rewritten against it; one
                                                                         # profile call each, one more for those flagged

Every rewrite applies the ticker's verified share count (ticker.verified_shares), as the live
valuation does, so the rewritten history and the next day's value agree.

Writes a report (summary counts, tickers with mismatches, skipped tickers) to
.output/data_fill_ticker_value_refresh_report.md.
"""
import argparse
import atexit
import os
from datetime import date, datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

from modules.object.exit import cleanup
from modules.object import ticker
from modules.object.ticker import Ticker
from modules.object.ticker_value import TickerValue, fetch_price_and_cap_series_between, fetch_values_for_ticker, replace_range
from modules.core import api_stocks
from modules.ticker import refresh
from modules.ticker.resolver import TickerResolver
from modules.ticker import company, pricing
from modules.ticker import util as tu

atexit.register(cleanup)

INCEPTION_DATE = date(2026, 1, 1)  # matches scripts/data_fill_ticker_value_gaps.py::FILL_START_DATE
REPORT_PATH = os.path.join(os.path.dirname(__file__), '..', '.output', 'data_fill_ticker_value_refresh_report.md')


def tickers_with_stored_outliers(tickers: list[Ticker], end_date: date) -> list[Ticker]:
    """The tickers whose stored market caps since INCEPTION_DATE contain an FMP glitch."""
    out: list[Ticker] = []
    for i in range(0, len(tickers), 500):
        chunk = tickers[i:i + 500]
        stored = fetch_price_and_cap_series_between([t.id for t in chunk], INCEPTION_DATE, end_date)
        out += [t for t in chunk if t.id in stored and pricing.market_cap_outliers(*stored[t.id])]
    return out


def tickers_off_their_profile(tickers: list[Ticker], resolver: TickerResolver) -> dict[int, float]:
    """{ticker_id: the profile's share count} for the tickers whose latest stored market cap is
    GLITCH_FACTOR-fold off their FMP profile's (refresh.stored_cap_off_profile)."""
    today = date.today()
    stored: dict = {}
    for i in range(0, len(tickers), 500):
        stored.update(fetch_price_and_cap_series_between([t.id for t in tickers[i:i + 500]], INCEPTION_DATE, today))

    def check(t: Ticker) -> tuple[int, float] | None:
        profile = api_stocks.get_stock_profile(resolver.get_full_symbol(t))
        if not isinstance(profile, dict):
            return None
        reason = refresh.stored_cap_off_profile(stored.get(t.id), profile, tu.listing_currency(t.exchange, t.currency), today)
        if not reason:
            return None
        print(f"  {t.symbol}:{t.exchange} {reason}")
        return t.id, profile['marketCap'] / profile['price']

    with ThreadPoolExecutor(max_workers=5) as executor:
        return dict(r for r in executor.map(check, tickers) if r)


def tickers_with_share_count_change(tickers: list[Ticker], resolver: TickerResolver) -> dict[int, float | None]:
    """{ticker_id: its new verified_shares (None = cleared)} for the tickers whose verified share
    count refresh.verify_share_count sets, changes or clears."""
    today = date.today()
    stored: dict = {}
    for i in range(0, len(tickers), 500):
        stored.update(fetch_price_and_cap_series_between([t.id for t in tickers[i:i + 500]], INCEPTION_DATE, today))

    def check(t: Ticker) -> tuple[int, float | None] | None:
        caps = stored.get(t.id, ({}, {}))[0]
        if t.verified_shares is None and (len(caps) < refresh.SHARES_CHECK_POINTS or max(caps) < today - timedelta(days=30)):
            return None  # can't trigger: too little recent history to judge (saves the profile call)
        full_symbol = resolver.get_full_symbol(t)
        profile = api_stocks.get_stock_profile(full_symbol)
        if not isinstance(profile, dict):
            return None
        verified, why = refresh.verify_share_count(
            t, full_symbol, profile, stored.get(t.id), tu.listing_currency(t.exchange, t.currency), today)
        if not why:
            return None
        print(f"  {t.symbol}:{t.exchange} {why}")
        return t.id, verified

    with ThreadPoolExecutor(max_workers=5) as executor:
        return dict(r for r in executor.map(check, tickers) if r)


def resync_ticker(t: Ticker, resolver: TickerResolver, end_date: date, dry_run: bool, reference_shares: float | None = None,
                  verified_shares: float | None = None):
    """Returns (symbol, rows_written, mismatches, error)."""
    full_symbol = resolver.get_full_symbol(t)
    fetched = pricing.fetch_price_and_market_cap_history(
        full_symbol, INCEPTION_DATE, end_date, currency=tu.listing_currency(t.exchange, t.currency),
        reference_shares=reference_shares, verified_shares=verified_shares,
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
    parser.add_argument('--currency-mismatch', action='store_true',
                        help="Only valid tickers whose FMP currency differs from their exchange's currency.")
    parser.add_argument('--outliers', action='store_true',
                        help="Only valid tickers whose stored market caps contain an FMP glitch.")
    parser.add_argument('--profile-check', action='store_true',
                        help="Only valid tickers with a stored market cap 2.5x+ off what their FMP profile's share count gives.")
    parser.add_argument('--shares-check', action='store_true',
                        help="Only valid tickers whose verified share count is set, changed or cleared (quote and financials against FMP's history).")
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
    elif args.currency_mismatch:
        tickers = [t for t in ticker.fetch_all_valid() if company.is_foreign_currency_line(t)]
    elif args.outliers:
        tickers = tickers_with_stored_outliers(ticker.fetch_all_valid(), date.today())
    elif args.profile_check:
        candidates = ticker.fetch_all_valid()
        print(f"Checking {len(candidates)} ticker(s) against their FMP profile...")
        references = tickers_off_their_profile(candidates, TickerResolver(TickerResolver.POPULATE_TICKER))
        tickers = [t for t in candidates if t.id in references]
    elif args.shares_check:
        candidates = ticker.fetch_all_valid()
        print(f"Checking {len(candidates)} ticker(s)' stored share counts against their FMP quote and financials...")
        verified = tickers_with_share_count_change(candidates, TickerResolver(TickerResolver.POPULATE_TICKER))
        tickers = [t for t in candidates if t.id in verified]
    else:
        tickers = ticker.fetch_all_valid()
    references = references if args.profile_check else {}
    verified = verified if args.shares_check else {}

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
    if not args.dry_run:
        for tid, shares in verified.items():
            ticker.update_verified_shares(tid, shares)

    processed = 0
    skipped = 0
    total_rows = 0
    tickers_with_diffs = 0
    mismatch_lines: list[str] = []
    skipped_lines: list[str] = []

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(resync_ticker, t, resolver, end_date, args.dry_run, references.get(t.id),
                                   verified.get(t.id, t.verified_shares)): t for t in tickers}
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
