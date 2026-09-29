import log
from datetime import date, timedelta
from modules.core import api_stocks
from modules.object import ticker, ticker_value
from modules.object.ticker import Ticker
from modules.ticker import company, pricing
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
            average_turnover=tu.profile_turnover(profile),
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
                result = pricing.resync_value_history(t.id, full_symbol, new_currency, verified_shares=t.verified_shares)
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
                    t.id, full_symbol, new_currency, reference_shares=profile['marketCap'] / profile['price'],
                    verified_shares=t.verified_shares)
                resynced += 1
                log.record_notice(f"Ticker profile refresh: {full_symbol} {reason}, value history rewritten ({result}).")
            else:
                verified, why = verify_share_count(t, full_symbol, profile, stored_values.get(t.id), new_currency, today)
                if why:
                    ticker.update_verified_shares(t.id, verified)
                    result = pricing.resync_value_history(t.id, full_symbol, new_currency, verified_shares=verified)
                    resynced += 1
                    log.record_notice(f"Ticker profile refresh: {full_symbol} {why}, value history rewritten ({result}).")

    log.record_status(
        f"Ticker profile refresh: {updated} updated, {marked_invalid} marked invalid, "
        f"{resynced} value histories rewritten (currency change or far off the profile), out of {len(tickers)} checked."
    )
    return len(tickers), updated, marked_invalid


SHARES_CHECK_POINTS = 10   # the latest stored values stored_shares_off_profile looks at
FINANCIALS_TOLERANCE = 0.05  # FMP's quarterly weighted shares within this of the profile's count confirm it
INDEX_TOLERANCE = 0.15  # the index funds' float cap within this of the quote's cap x free float confirms the quote


def _latest_usd_rate(currency: str | None, today: date) -> float | None:
    if not currency or currency == 'USD':
        return 1.0
    rates = pricing.fetch_historic_usd_rates(currency, today - timedelta(days=10), today)
    return rates[max(rates)] if isinstance(rates, dict) and rates else None


def stored_shares_off_profile(
    stored: tuple[dict[date, float], dict[date, float]] | None, profile: dict, currency: str | None, today: date,
) -> bool:
    """Whether the latest SHARES_CHECK_POINTS stored market caps (USD; `stored` = ({date: cap},
    {date: price})) all imply a share count between SHARE_COUNT_TOLERANCE- and
    REFERENCE_FACTOR-fold off the profile's, on the same side — FMP's history carried on another
    count than its quote throughout (Rocket Companies: 3.79B against 2.82B), too close for the
    glitch filter; stored_cap_off_profile handles REFERENCE_FACTOR and beyond."""
    cap, price = profile.get('marketCap'), profile.get('price')
    if not stored or not cap or not price or cap <= 0 or price <= 0:
        return False
    rate = _latest_usd_rate(currency, today)
    if not rate:
        return False
    shares = cap / price
    caps, prices = stored
    recent = sorted(d for d, v in caps.items() if v and v > 0 and prices.get(d))[-SHARES_CHECK_POINTS:]
    if len(recent) < SHARES_CHECK_POINTS:
        return False
    ratios = [caps[d] / (prices[d] * rate) / shares for d in recent]
    return (all(pricing.SHARE_COUNT_TOLERANCE <= r < pricing.REFERENCE_FACTOR for r in ratios)
            or all(pricing.SHARE_COUNT_TOLERANCE <= 1 / r < pricing.REFERENCE_FACTOR for r in ratios))


def index_backs_quote(
    t: Ticker, profile: dict, stored: tuple[dict[date, float], dict[date, float]] | None,
    currency: str | None, today: date,
) -> bool:
    """Whether the index funds' holding backs the quote's market cap: their float cap
    (ticker.float_factor — the index's float cap over our company cap, modules/ticker/free_float.py
    — times our latest stored cap) within INDEX_TOLERANCE of the quote's cap x its free float. A
    third source when FMP's financials still show an old share count (Omnicom and Devon Energy
    after their mergers: history and financials on the old count, the quote and the index on the
    new one). Only for a company's own row (a master or standalone ticker), the cap the factor was
    measured against."""
    if t.master_ticker_id is not None or not t.float_factor or not t.free_float or not stored or not stored[0]:
        return False
    cap, rate = profile.get('marketCap'), _latest_usd_rate(currency, today)
    if not cap or cap <= 0 or not rate:
        return False
    caps = stored[0]
    index_float_cap = t.float_factor * caps[max(caps)]
    quote_float_cap = cap * rate * t.free_float / 100
    return abs(index_float_cap / quote_float_cap - 1) <= INDEX_TOLERANCE


def verify_share_count(
    t: Ticker, full_symbol: str, profile: dict, stored: tuple[dict[date, float], dict[date, float]] | None,
    currency: str | None, today: date,
) -> tuple[float | None, str | None]:
    """(the ticker's verified_shares after this check, what changed — None when nothing did).

    When the stored history is off the profile's share count (stored_shares_off_profile), or the
    ticker already has a verified count, a second source breaks the tie: FMP's quarterly
    financials (their weighted share count within FINANCIALS_TOLERANCE of the profile's), else the
    index funds (index_backs_quote). Either makes the profile's count verified — two sources
    against FMP's history, which pricing.clean_market_caps then repairs on every fetch. The profile
    isn't trusted alone: it can be the wrong side (SABESP), and then the financials and the index
    agree with the history instead. A verified count follows the profile while a second source
    agrees, and is cleared when neither does. A line quoted in pence has a profile
    count 1/100 of the real one (FMP's unit), which is allowed for. Preferred / note / unit /
    depositary lines are left out: their quote and financials are the parent company's (Duke
    Energy's units DUKU), and they never set a company's cap."""
    if company.is_secondary_line(t):
        return None, ("verified share count cleared (not an ordinary-share line)" if t.verified_shares is not None else None)
    cap, price = profile.get('marketCap'), profile.get('price')
    if not cap or not price or cap <= 0 or price <= 0:
        return t.verified_shares, None
    shares = cap / price
    if t.verified_shares is None and not stored_shares_off_profile(stored, profile, currency, today):
        return None, None
    financial = api_stocks.get_quarterly_weighted_shares(full_symbol)
    unit = tu.minor_unit_factor(profile.get('currency'))
    if financial and abs(financial / (shares * unit) - 1) <= FINANCIALS_TOLERANCE:
        witness = 'financials'
    elif index_backs_quote(t, profile, stored, currency, today):
        witness = 'index funds'
    else:
        witness = None
    if witness:
        if t.verified_shares and abs(shares / t.verified_shares - 1) < 0.005:
            return t.verified_shares, None
        verb = 'set' if t.verified_shares is None else f'updated from {t.verified_shares:,.0f}'
        return shares, f"verified share count {verb} to {shares:,.0f} (quote and {witness} agree against FMP's history)"
    if t.verified_shares is not None:
        return None, "verified share count cleared (neither the financials nor the index funds agree with the quote)"
    return None, None


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
