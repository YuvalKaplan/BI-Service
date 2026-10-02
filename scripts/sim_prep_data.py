"""
Prepares ticker data needed before a historical simulation, in the live generation day's order:
screens today's FMP large caps (stored, dated at the data cutoff), refreshes stale ticker
profile data (cik/isin/name/etc., needed for master-ticker grouping), values the tickers in use
and the screen for that date, groups listings into companies (master tickers) and refreshes
company_market_cap and region, refreshes free floats and float factors (modules/ticker/free_float.py),
then builds the screened companies. Follow with
scripts/sim_benchmark.py (historical benchmark backfill), then sim_fund.py.

Every step is idempotent, so it's safe to re-run this even if the live pipeline has already
populated the data — it'll just pick up anything new/stale since last time.

Writes a combined report (screener, ticker profile refresh, values, master ticker groups, companies)
to .output/sim_prep_data_report.md.

Usage:
    python scripts/sim_prep_data.py --dev
    python scripts/sim_prep_data.py --prod
"""
import atexit
import os
from datetime import datetime
from modules.object.exit import cleanup
from modules.cron import screener, company_builder
from modules.sim.benchmark_generator import data_cutoff_date
from modules.ticker import free_float, master, refresh, valuation

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
        screen = screener.run(screen_date=screen_date, store=True)
        print(screener.summary(screen))
        report_sections.append("\n".join(
            ["", "## Screener", "", screener.summary(screen)]
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

        floats = free_float.refresh()
        print(free_float.summary(floats))
        report_sections.append("\n".join(
            ["", "## Free Float", "", free_float.summary(floats)]
            + _section("Listings with no equity float (notes / preferreds, left out of the companies)", floats.zero_float)))

        # require_value=False: the backfill re-fetches every listing's history, so a withheld or
        # failed value on the screen date mustn't drop a company from every historical generation day.
        companies = company_builder.run(screen_date=screen_date, require_value=False)
        print(company_builder.summary(companies))
        report_sections.append("\n".join(
            ["", "## Screened Companies", "", company_builder.summary(companies)]
            + _section("Companies dropped by the duplicate guard", companies.duplicate_companies)
            + _section("Note / preferred lines left out", companies.non_equity)
            + _section("Foreign lines admitted (company found only outside its home market)", companies.foreign_admitted)
            + _section("Foreign lines skipped as duplicates of a company already in", companies.foreign_duplicates)
            + _section("Foreign lines skipped as quoted in another currency (mirrored data)", companies.foreign_currency)
            + _section("Listings without any stored value", companies.no_value)
        ))

        report = "\n\n---\n".join(report_sections)
        os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
        with open(REPORT_PATH, 'w', encoding='utf-8') as f:
            f.write(report)
        print(f"\nReport written to: {REPORT_PATH}")

    except Exception as e:
        print(f"Error in sim data prep: {e}")
