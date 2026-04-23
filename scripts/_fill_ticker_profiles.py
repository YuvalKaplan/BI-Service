import atexit
from modules.object.exit import cleanup
from modules.object.ticker import Ticker, fetch_with_missing_exchange, update_profile, update_invalid
from modules.ticker import util as tu
from modules.ticker.resolver import populate_esg
from modules.core.api_stocks import get_stock_profile

atexit.register(cleanup)


def _names_match(db_name: str | None, api_name: str | None) -> bool:
    if not db_name or not api_name:
        return False
    return db_name.strip().lower() == api_name.strip().lower()


if __name__ == "__main__":
    tickers = fetch_with_missing_exchange()
    print(f"Found {len(tickers)} tickers with missing exchange.\n")

    updated = 0
    skipped_no_match = 0
    skipped_api_error = 0

    for t in tickers:
        assert t.id is not None

        profile = get_stock_profile(t.symbol)
        if isinstance(profile, str):
            print(f"  [{t.symbol}] API error: {profile}")
            skipped_api_error += 1
            continue

        api_name = profile.get('companyName')
        if not _names_match(t.name, api_name):
            print(f"  [{t.symbol}] Name mismatch — DB: '{t.name}' / API: '{api_name}' — skipping")
            skipped_no_match += 1
            continue

        exchange = profile.get('exchange')

        if exchange == 'CRYPTO':
            update_invalid(t.id, 'Crypto')
            print(f"  [{t.symbol}] Marked invalid: Crypto")
            continue

        if not api_name or tu.is_unwanted_names(api_name):
            update_invalid(t.id, 'Fund or ETF')
            print(f"  [{t.symbol}] Marked invalid: Fund or ETF")
            continue

        updated_ticker = Ticker(
            symbol=t.symbol,
            isin=t.isin or profile.get('isin'),
            cusip=profile.get('cusip'),
            cik=profile.get('cik'),
            name=api_name,
            exchange=exchange,
            industry=profile.get('industry'),
            sector=profile.get('sector'),
            country=profile.get('country'),
            currency=profile.get('currency'),
            source='fmp',
            type_from=t.type_from,
        )
        update_profile(t.id, updated_ticker)
        populate_esg(t.id, t.symbol)
        print(f"  [{t.symbol}] Updated Profile (exchange={exchange})")
        updated += 1

    print(f"\nDone. Updated: {updated} | Skipped name mismatch: {skipped_no_match} | API errors: {skipped_api_error}")
