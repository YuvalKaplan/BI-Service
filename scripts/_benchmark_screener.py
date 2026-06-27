import atexit
import re
from modules.object.exit import cleanup
from modules.core import api_stocks
from modules.object import ticker

atexit.register(cleanup)

MARKET_CAP_THRESHOLD = 10_000_000_000  # $10B

if __name__ == '__main__':
    print("Fetching large-cap universe from FMP screener...")
    results: list[dict] = []
    for page in range(api_stocks.SCREENER_MAX_PAGES):
        page_data = api_stocks.fetch_company_screener(
            market_cap_more_than=MARKET_CAP_THRESHOLD,
            page=page,
            limit=api_stocks.SCREENER_PAGE_LIMIT,
        )
        results.extend(page_data)
        print(f"  Page {page}: {len(page_data)} results")
        if len(page_data) < api_stocks.SCREENER_PAGE_LIMIT:
            break

    print(f"\nTotal companies from screener: {len(results)}")

    symbol_cache = ticker.fetch_all_for_symbol_cache()
    print(f"Existing tickers in DB: {len(symbol_cache)}")

    us_new, us_existing = [], []
    intl_new, intl_existing = [], []
    skipped = 0

    for company in results:
        raw_symbol = company.get('symbol')
        market_cap = company.get('marketCap')
        country = company.get('country') or ''

        if not raw_symbol or not market_cap or market_cap <= 0:
            skipped += 1
            continue

        symbol = re.split(r'[\s.]', raw_symbol)[0]
        is_new = symbol not in symbol_cache
        bucket_new = us_new if country == 'US' else intl_new
        bucket_existing = us_existing if country == 'US' else intl_existing
        (bucket_new if is_new else bucket_existing).append(symbol)

    print(f"\n{'Region':<20} {'Existing':>10} {'New':>10} {'Total':>10}")
    print("-" * 52)
    print(f"{'US':<20} {len(us_existing):>10} {len(us_new):>10} {len(us_existing)+len(us_new):>10}")
    print(f"{'International':<20} {len(intl_existing):>10} {len(intl_new):>10} {len(intl_existing)+len(intl_new):>10}")
    print("-" * 52)
    total_existing = len(us_existing) + len(intl_existing)
    total_new = len(us_new) + len(intl_new)
    print(f"{'Total':<20} {total_existing:>10} {total_new:>10} {total_existing+total_new:>10}")

    if skipped:
        print(f"\nSkipped (missing symbol or market cap): {skipped}")

    if us_new:
        print(f"\nNew US tickers ({len(us_new)}): {', '.join(us_new[:20])}" + (" ..." if len(us_new) > 20 else ""))
    if intl_new:
        print(f"New Intl tickers ({len(intl_new)}): {', '.join(intl_new[:20])}" + (" ..." if len(intl_new) > 20 else ""))
