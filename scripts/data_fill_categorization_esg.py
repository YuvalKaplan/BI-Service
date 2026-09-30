"""
Fills missing style (value/growth) classification and ESG data for valid companies (masters and
standalone tickers; share-class siblings are skipped, as in the live pipeline).

Steps:
  1. (optional) Re-read the style reference ETFs' FMP holdings into categorize_ticker (slow).
  2. Style chain for tickers with style_type IS NULL: CAT_ETF match -> PROVIDER_ETF -> MODEL
     (GradientBoosting). Optionally clears the 30-day back-off on tickers whose factor fetch
     failed before, so the model retries them now.
  3. ESG for companies never fetched (esg_factors IS NULL), or for every company if asked.

Run data_fill_ticker_profile.py and data_fill_master_tickers.py first, so siblings are linked and
only masters get classified. Writes a before/after coverage report to
.output/data_fill_categorization_esg_report.md.

Usage:
    python scripts/data_fill_categorization_esg.py --dev
    python scripts/data_fill_categorization_esg.py --prod
"""
import atexit
import os
from datetime import datetime
from modules.object.exit import cleanup
from modules.cron import categorize_downloader
from modules.object import ticker
from modules.object import categorize_ticker as cat_ticker_obj
from modules.calc import classification
from modules.ticker.esg import populate_esg
from modules.ticker.resolver import TickerResolver

atexit.register(cleanup)

REPORT_PATH = os.path.join(os.path.dirname(__file__), '..', '.output', 'data_fill_categorization_esg_report.md')

STAT_LABELS = [
    ('companies',            'Valid companies'),
    ('style_missing',        'Style missing'),
    ('style_factors_failed', '  of which factor fetch failed (retry back-off)'),
    ('style_cat_etf',        'Style from CAT_ETF'),
    ('style_provider_etf',   'Style from PROVIDER_ETF'),
    ('style_model',          'Style from MODEL'),
    ('esg_missing',          'ESG never fetched'),
    ('esg_no_data',          'ESG fetched, no FMP data'),
    ('esg_qualified',        'ESG qualified'),
]


def ask(question: str) -> bool:
    return input(f"{question} [y/N] ").strip().lower() == 'y'


def fill_style(retry_failed: bool) -> None:
    if retry_failed:
        cleared = ticker.clear_style_factors_failed_at()
        print(f"Cleared factor-fetch back-off on {cleared} ticker(s).")

    ticker.update_style_for_unclassified()
    ticker.update_style_from_provider_etfs()
    print("Style assigned from categorization ETF matches and provider ETFs.")

    training_data = cat_ticker_obj.fetch_all_for_style_classification()
    if not training_data:
        print("No categorize_ticker training data - skipping the model classifier.")
        return
    items = [classification.to_categorize_ticker_item(t) for t in training_data]
    classifier = classification.get_classifier(items)
    classification.mark_style(classifier, ticker)
    print("Model classifier run for remaining unclassified tickers.")


def fill_esg(refresh_all: bool) -> int:
    companies = ticker.fetch_all_valid_companies() if refresh_all else ticker.fetch_companies_missing_esg()
    print(f"ESG: fetching {len(companies)} companies...")
    resolver = TickerResolver(TickerResolver.POPULATE_TICKER)
    for i, t in enumerate(companies, 1):
        populate_esg(t.id, resolver.get_full_symbol(t))
        if i % 100 == 0:
            print(f"  {i}/{len(companies)}")
    return len(companies)


def build_report(before: dict, after: dict, esg_count: int, options: list[str]) -> str:
    lines = [
        "# Categorization & ESG Data Fill Report",
        "",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "Options: " + (", ".join(options) if options else "none"),
        f"ESG fetched for: {esg_count} companies",
        "",
        "| Metric | Before | After |",
        "|--------|-------:|------:|",
    ]
    for key, label in STAT_LABELS:
        lines.append(f"| {label} | {before.get(key, 0)} | {after.get(key, 0)} |")
    return "\n".join(lines)


if __name__ == '__main__':
    try:
        reload_style_etfs = ask("Re-read the style reference ETFs' holdings from FMP first (slow)?")
        retry_failed = ask("Retry tickers whose style factor fetch failed before (ignore the 30-day back-off)?")
        refresh_esg = ask("Refresh ESG for ALL companies (not only those never fetched)?")
        options = [name for name, on in [
            ('re-read style reference ETFs', reload_style_etfs),
            ('retry failed style factors', retry_failed),
            ('refresh all ESG', refresh_esg),
        ] if on]

        before = ticker.fetch_style_esg_stats()

        if reload_style_etfs:
            categorize_downloader.run()
        fill_style(retry_failed)
        esg_count = fill_esg(refresh_esg)

        after = ticker.fetch_style_esg_stats()
        report = build_report(before, after, esg_count, options)
        print(f"\n{report}")

        os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
        with open(REPORT_PATH, 'w', encoding='utf-8') as f:
            f.write(report)
        print(f"\nReport written to: {REPORT_PATH}")

    except Exception as e:
        print(f"Error in categorization/ESG data fill: {e}")
