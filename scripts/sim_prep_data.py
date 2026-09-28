"""
Prepares ticker data needed before a historical simulation, in the live Wednesday order:
screens today's FMP large-cap universe (stored, dated at the data cutoff), refreshes stale ticker
profile data (cik/isin/name/etc., needed for master-ticker grouping), values the tickers in use
and the screen for that date, groups listings into companies (master tickers) and refreshes
company_market_cap and region, then builds the large-cap company universe. Follow with
scripts/sim_benchmark.py (historical benchmark backfill), then sim_fund.py.

Every step is idempotent, so it's safe to re-run this even if the live pipeline has already
populated the data — it'll just pick up anything new/stale since last time.

Writes a combined report (screener, ticker profile refresh, values, master ticker groups, universe)
to .output/sim_prep_data_report.md.

Usage:
    python scripts/sim_prep_data.py --dev
    python scripts/sim_prep_data.py --prod
"""
import atexit
import os
from datetime import datetime
from modules.object.exit import cleanup
from modules.cron import universe_screener, universe_builder
from modules.sim.benchmark_generator import data_cutoff_date
from modules.ticker import master, refresh, valuation

atexit.register(cleanup)

REPORT_PATH = os.path.join(os.path.dirname(__file__), '..', '.output', 'sim_prep_data_report.md')


def _section(title: str, items: list[str]) -> list[str]:
    return ["", f"### {title}: {len(items)}", ""] + ([f"- {x}" for x in items] or ["None."])


if __name__ == '__main__':
    try:
        include_invalid = input("Also retry tickers previously marked invalid? [y/N] ").strip().lower() == 'y'

        report_sections = ["\n".join([
            "# Sim Data Prep Report",
            "",
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        ])]

        # Dated at the data cutoff (a few days back) rather than the latest trading day, so FMP's
        # historical endpoints have every market's value for it.
        screen_date = data_cutoff_date()
        screen = universe_screener.run(screen_date=screen_date, store=True)
        print(universe_screener.summary(screen))
        report_sections.append("\n".join(
            ["", "## Universe Screener", "", universe_screener.summary(screen)]
            + _section("Non-equity lines skipped (preferred, notes, …)", screen.non_equity)
        ))

        total, updated, marked_invalid = refresh.refresh_ticker_profiles(include_invalid=include_invalid)
        print(f"Ticker profile refresh: {updated} updated, {marked_invalid} marked invalid, out of {total} checked.")
        report_sections.append("\n".join([
            "", "## Ticker Profile Refresh", "",
            f"Checked: {total} | Updated: {updated} | Marked invalid: {marked_invalid}",
        ]))

        values = valuation.run(value_date=screen_date)
        print(valuation.summary(values))
        report_sections.append("\n".join(["", "## Ticker Values", "", valuation.summary(values)]))

        masters_updated, unlinked, caps_updated = master.sync_masters_and_company_data()
        print(f"Master sync: {masters_updated} link(s), {unlinked} unlinked, {caps_updated} company cap(s) refreshed.")
        groups_report = master.build_master_groups_report(masters_updated, caps_updated, unlinked)
        print(f"\n{groups_report}")
        report_sections.append(groups_report)

        # require_value=False: the backfill re-fetches every listing's history, so a withheld or
        # failed value on the screen date mustn't drop a company from every historical Wednesday.
        universe = universe_builder.run(screen_date=screen_date, require_value=False)
        print(universe_builder.summary(universe))
        report_sections.append("\n".join(
            ["", "## Large-Cap Universe", "", universe_builder.summary(universe)]
            + _section("Companies dropped by the duplicate guard", universe.duplicate_companies)
            + _section("Foreign lines admitted (company found only outside its home market)", universe.foreign_admitted)
            + _section("Foreign lines skipped as duplicates of a company already in", universe.foreign_duplicates)
            + _section("Foreign lines skipped as quoted in another currency (mirrored data)", universe.foreign_currency)
            + _section("Listings without any stored value", universe.no_value)
        ))

        report = "\n\n---\n".join(report_sections)
        os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
        with open(REPORT_PATH, 'w', encoding='utf-8') as f:
            f.write(report)
        print(f"\nReport written to: {REPORT_PATH}")

    except Exception as e:
        print(f"Error in sim data prep: {e}")
