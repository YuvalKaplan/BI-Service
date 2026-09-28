import log
from datetime import date, timedelta
from modules.core import api_stocks
from modules.object import ticker, ticker_value
from modules.object.ticker import Ticker
from modules.ticker import pricing
from modules.ticker import util as tu
from modules.ticker.resolver import TickerResolver


def refresh_ticker_profiles(include_invalid: bool = False) -> tuple[int, int, int]:
    """
    Refreshes full ticker profile data (isin/cusip/cik/name/industry/sector/country/currency/
    is_actively_trading) from FMP for every ticker whose profile hasn't been checked within
    the last week (ticker.fetch_stale_tickers) — the single shared implementation used by
    scripts/data_fill_ticker_profile.py, scripts/sim_prep_data.py, and the live Tue-Sat cron
    step, so all three go over the exact same ticker list rather than each having their own
    narrower variant (e.g. the old cik-only backfill).

    A ticker is flagged invalid if its profile turns out to be crypto, a fund/ETF, or no
    longer actively trading — same detection as the original data_fill_ticker_profile.py.
    A ticker whose profile lookup itself returns an invalid/error response is also marked
    invalid (rather than merely logged and retried forever), since fetch_stale_tickers()
    already excludes already-invalid tickers by default — pass include_invalid=True to retry
    them too (a ticker that checks out fine on retry has its invalid flag cleared).

    Returns (total_checked, updated, marked_invalid).
    """
    resolver = TickerResolver(TickerResolver.POPULATE_TICKER)
    tickers = ticker.fetch_stale_tickers(include_invalid=include_invalid)

    updated = 0
    marked_invalid = 0
    resynced = 0
    today = date.today()
    stored_values = ticker_value.fetch_price_and_cap_series_between([t.id for t in tickers], pricing.VALUE_HISTORY_START, today)

    for t in tickers:
        full_symbol = resolver.get_full_symbol(t)
        profile = api_stocks.get_stock_profile(full_symbol)
        if not isinstance(profile, dict):
            log.record_notice(f"Ticker profile refresh: no profile for '{full_symbol}': {profile}")
            ticker.update_invalid(t.id, "Profile lookup failed")
            marked_invalid += 1
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
        ticker.update(updated_ticker)  # stamps updated_at

        ticker.update_invalid(t.id, invalid_reason)  # clears it if the retry now checks out fine
        if invalid_reason:
            marked_invalid += 1
        else:
            updated += 1
            # Values are converted to USD from the listing's currency (util.listing_currency):
            # when that changes, the stored history was converted from the old one.
            new_currency = tu.listing_currency(t.exchange, updated_ticker.currency)
            if new_currency != tu.listing_currency(t.exchange, t.currency):
                result = pricing.resync_value_history(t.id, full_symbol, new_currency)
                resynced += 1
                log.record_notice(
                    f"Ticker profile refresh: {full_symbol} currency {t.currency} -> {updated_ticker.currency}, "
                    f"value history rewritten ({result})."
                )
            elif (reason := stored_cap_off_profile(stored_values.get(t.id), profile, new_currency, today)):
                # A history FMP carries on a wrong share count throughout shows no jump for the
                # glitch filter to catch (Uniper's new XETRA line at 8.7B shares against 416M);
                # the profile fetched here anyway is the reference to rewrite it against.
                result = pricing.resync_value_history(
                    t.id, full_symbol, new_currency, reference_shares=profile['marketCap'] / profile['price'])
                resynced += 1
                log.record_notice(f"Ticker profile refresh: {full_symbol} {reason}, value history rewritten ({result}).")

    log.record_status(
        f"Ticker profile refresh: {updated} updated, {marked_invalid} marked invalid, "
        f"{resynced} value histories rewritten (currency change or far off the profile), out of {len(tickers)} checked."
    )
    return len(tickers), updated, marked_invalid


def stored_cap_off_profile(
    stored: tuple[dict[date, float], dict[date, float]] | None, profile: dict, currency: str | None, today: date,
) -> str | None:
    """Why some stored market caps (USD; `stored` = ({date: cap}, {date: price})) are
    pricing.REFERENCE_FACTOR-fold off what the profile's share count (market cap / price) gives at
    that date's stored price — converted at the latest FX rate, close enough for a 3-fold test —
    or None when none are, or data is missing. Catches a history on a wrong share count
    throughout (no jump for the glitch filter) and DuPont-like steps just under its threshold."""
    cap, price = profile.get('marketCap'), profile.get('price')
    if not stored or not cap or not price or cap <= 0 or price <= 0:
        return None
    rate = 1.0
    if currency and currency != 'USD':
        rates = pricing.fetch_historic_usd_rates(currency, today - timedelta(days=10), today)
        if isinstance(rates, str) or not rates:
            return None
        rate = rates[max(rates)]
    shares = cap / price
    caps, prices = stored
    off = sorted(d for d, v in caps.items()
                 if v and v > 0 and prices.get(d) and max(v, prices[d] * shares * rate) / min(v, prices[d] * shares * rate) >= pricing.REFERENCE_FACTOR)
    if not off:
        return None
    return f"{len(off)} stored cap(s) {off[0]} .. {off[-1]} 2.5x+ off the profile's share count"
