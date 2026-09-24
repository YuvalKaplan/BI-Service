import atexit
from modules.object.exit import cleanup
from modules.object.ticker import Ticker, fetch_all_valid, update, update_invalid
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
        name = profile.get('companyName')
        is_active = profile.get('isActivelyTrading')

        invalid_reason = None
        if exchange == 'CRYPTO':
            invalid_reason = 'Crypto'
        elif not name or tu.is_unwanted_names(name):
            invalid_reason = 'Fund or ETF'
        elif is_active is not None and not is_active:
            invalid_reason = 'Not actively trading'

        updated_ticker = Ticker(
            id=t.id,
            symbol=t.symbol,
            isin=profile.get('isin') or t.isin,
            cusip=profile.get('cusip') or t.cusip,
            cik=profile.get('cik') or t.cik,
            name=name or t.name,
            exchange=t.exchange,
            industry=profile.get('industry') or t.industry,
            sector=profile.get('sector') or t.sector,
            country=profile.get('country') or t.country,
            currency=profile.get('currency') or t.currency,
            source=t.source,
            type_from=t.type_from,
            is_actively_trading=bool(is_active) if is_active is not None else None,
        )
        update(updated_ticker)

        update_invalid(t.id, invalid_reason)
        if invalid_reason:
            print(f"  [{full_symbol}] Marked invalid: {invalid_reason}")
            marked_invalid += 1
        else:
            print(f"  [{full_symbol}] Updated")
            updated += 1

    print(f"\nDone. Updated: {updated} | Marked invalid: {marked_invalid} | Skipped: {skipped}")
