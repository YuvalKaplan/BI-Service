import atexit
from modules.object.exit import cleanup
from modules.object.ticker import Ticker, fetch_all_valid, update_profile, update_invalid
from modules.ticker import util as tu
from modules.ticker.resolver import TickerResolver
from modules.core.api_stocks import get_stock_profile

atexit.register(cleanup)


if __name__ == "__main__":
    tickers = fetch_all_valid()
    print(f"Updating {len(tickers)} tickers.\n")

    resolver = TickerResolver(TickerResolver.POPULATE_TICKER)
    updated = 0
    marked_invalid = 0
    skipped = 0

    for t in tickers:
        assert t.id is not None

        full_symbol = resolver.get_full_symbol(t)
        profile = get_stock_profile(full_symbol)
        if not isinstance(profile, dict):
            print(f"  [{full_symbol}] API error: {profile}")
            skipped += 1
            continue

        exchange = profile.get('exchange')
        if exchange == 'CRYPTO':
            update_invalid(t.id, 'Crypto')
            print(f"  [{full_symbol}] Marked invalid: Crypto")
            marked_invalid += 1
            continue

        name = profile.get('companyName')
        if not name or tu.is_unwanted_names(name):
            update_invalid(t.id, 'Fund or ETF')
            print(f"  [{full_symbol}] Marked invalid: Fund or ETF")
            marked_invalid += 1
            continue

        is_active = profile.get('isActivelyTrading')
        if is_active is not None and not is_active:
            update_invalid(t.id, 'Not actively trading')
            print(f"  [{full_symbol}] Marked invalid: not actively trading")
            marked_invalid += 1
            continue

        country = profile.get('country')
        if not country:
            print(f"  [{full_symbol}] No country in profile — skipping")
            skipped += 1
            continue

        updated_ticker = Ticker(
            symbol=t.symbol,
            isin=t.isin or profile.get('isin'),
            cusip=t.cusip or profile.get('cusip'),
            cik=t.cik or profile.get('cik'),
            name=name,
            exchange=t.exchange,
            industry=t.industry or profile.get('industry'),
            sector=t.sector or profile.get('sector'),
            country=country,
            currency=t.currency or profile.get('currency'),
            source=t.source,
            type_from=t.type_from,
        )
        update_profile(t.id, updated_ticker)
        print(f"  [{full_symbol}] Updated (country={country})")
        updated += 1

    print(f"\nDone. Updated: {updated} | Marked invalid: {marked_invalid} | Skipped: {skipped}")
