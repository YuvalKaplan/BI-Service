import re
import log
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

from modules.core import api_stocks
from modules.ticker import util as tu
from modules.object.ticker import Ticker, fetch_by_symbol, upsert_by_symbol, update_invalid, update_esg_data
from modules.object.ticker_value import TickerValue, upsert as _upsert_tv
from modules.calc import esg as _esg

_VALUE_DATE_CUT_OFF_HOUR = 17
_symbol_cache:        dict[str, int | None] = {}
_isin_cache:          dict[str, int | None] = {}
_exchange_suffix_map: dict[str, str]        = {}  # exchange code → symbolSuffix, loaded once


def resolve(
    region: str,
    symbol: str | None,
    isin: str | None = None,
    name: str | None = None,
) -> int | None:
    """Return the DB id for a holding row, resolving and storing all ticker data as needed."""
    if region == 'US':
        return _resolve_by_symbol(symbol)
    else:
        return _resolve_non_us(symbol, isin, name)


def _get_value_date() -> date:
    now_et = datetime.now(ZoneInfo("America/New_York"))
    return (now_et - timedelta(days=1) if now_et.hour < _VALUE_DATE_CUT_OFF_HOUR else now_et).date()


def _populate(profile: dict, symbol: str, isin: str | None) -> int | None:
    """Shared post-profile logic: upsert ticker, validate name, store value + ESG."""
    exchange = profile.get('exchange')
    ticker = Ticker(
        symbol=symbol,
        isin=isin or profile.get('isin'),
        cusip=profile.get('cusip'),
        cik=profile.get('cik'),
        name=profile.get('companyName'),
        exchange=exchange,
        industry=profile.get('industry'),
        sector=profile.get('sector'),
        currency=profile.get('currency'),
        source='fmp',
        type_from='holding',
    )
    ticker_id = upsert_by_symbol(ticker)

    if profile.get('exchange') == 'CRYPTO':
        update_invalid(symbol, 'Crypto')
        return None

    name = profile.get('companyName')
    if not name:
        update_invalid(symbol, 'Missing details')
        return None
    if tu.is_unwanted_names(name):
        update_invalid(symbol, 'Fund or ETF')
        return None

    _store_ticker_value(ticker_id, profile)
    _store_esg(symbol, exchange)
    return ticker_id


def _resolve_by_symbol(symbol: str | None) -> int | None:
    if not symbol:
        return None
    if symbol in _symbol_cache:
        return _symbol_cache[symbol]

    existing = fetch_by_symbol(symbol)
    if existing and existing.invalid:
        _symbol_cache[symbol] = None
        return None

    profile = api_stocks.get_stock_profile(symbol)
    if not isinstance(profile, dict):
        log.record_notice(f"No stocks data provider profile for symbol '{symbol}': {profile}")
        result = existing.id if existing else None
        _symbol_cache[symbol] = result
        return result

    result = _populate(profile, symbol, None)
    _symbol_cache[symbol] = result
    return result


def _resolve_non_us(symbol: str | None, isin: str | None, name: str | None = None) -> int | None:
    """Non-US path: prefer ISIN lookup; fall back to symbol search when ISIN is absent."""
    if isin:
        return _resolve_by_isin(isin)
    return _resolve_by_symbol_search(symbol, name)


def _resolve_by_isin(isin: str | None) -> int | None:
    if not isin:
        return None
    if isin in _isin_cache:
        return _isin_cache[isin]

    search_result = api_stocks.search_by_isin(isin)
    if not search_result:
        log.record_notice(f"No stocks data provider search result for ISIN '{isin}'")
        _isin_cache[isin] = None
        return None
    symbol_full = search_result.get('symbol')
    if not symbol_full:
        _isin_cache[isin] = None
        return None
    symbol = re.split(r'[\s.]', symbol_full)[0]

    existing = fetch_by_symbol(symbol)
    if existing and existing.invalid:
        _isin_cache[isin] = None
        return None

    profile = api_stocks.get_stock_profile(symbol_full)
    if not isinstance(profile, dict):
        log.record_notice(f"No stocks data provider profile for symbol '{symbol}' (ISIN '{isin}'): {profile}")
        result = existing.id if existing else None
        _isin_cache[isin] = result
        return result

    result = _populate(profile, symbol, isin)
    _isin_cache[isin] = result
    return result


def _resolve_by_symbol_search(symbol: str | None, name: str | None = None) -> int | None:
    """Resolve a non-US ticker that has no ISIN via FMP symbol search."""
    if not symbol:
        return None

    cache_key = symbol
    if cache_key in _symbol_cache:
        return _symbol_cache[cache_key]

    # Strip exchange suffix / qualifier from the raw symbol to form the search query.
    # "2330.T" → "2330", "700 HK" → "700", "A005930" → "A005930"
    query = re.split(r'[\s.]', symbol)[0]

    candidates = api_stocks.search_by_symbol(query)
    matched = tu.filter_symbol_candidates(candidates, query)

    result = None
    for candidate in matched:
        fmp_symbol_full = candidate.get('symbol')
        if not fmp_symbol_full:
            continue
        if name and not tu.names_match(name, candidate.get('name', '')):
            continue
        fmp_symbol = re.split(r'[\s.]', fmp_symbol_full)[0]
        profile = api_stocks.get_stock_profile(fmp_symbol_full)
        if not isinstance(profile, dict):
            continue
        result = _populate(profile, fmp_symbol, None)
        break

    if result is None and name:
        found_symbol = tu.resolve_ticker_from_alt_data(isin=None, name=name)
        if found_symbol:
            fmp_symbol = re.split(r'[\s.]', found_symbol)[0]
            profile = api_stocks.get_stock_profile(found_symbol)
            if isinstance(profile, dict):
                result = _populate(profile, fmp_symbol, None)

    if result is None:
        log.record_notice(f"No verified stocks data provider match for non-US symbol '{symbol}'")

    _symbol_cache[cache_key] = result
    return result


def _store_ticker_value(ticker_id: int, profile: dict) -> None:
    try:
        price = profile.get('price')
        market_cap = profile.get('marketCap')
        if price and market_cap:
            _upsert_tv(TickerValue(
                ticker_id=ticker_id,
                value_date=_get_value_date(),
                stock_price=float(price),
                market_cap=float(market_cap),
            ))
    except Exception as e:
        log.record_notice(f"Failed to store ticker_value for ticker_id={ticker_id}: {e}")


def _get_exchange_suffix(exchange: str | None) -> str:
    """Return the FMP symbol suffix for an exchange code (e.g. '.MU'), or '' for US/unknown."""
    if not exchange:
        return ''
    if not _exchange_suffix_map:
        for e in api_stocks.fetch_available_exchanges():
            code = e.get('exchange')
            suffix = e.get('symbolSuffix', '')
            if code:
                _exchange_suffix_map[code] = '' if suffix == 'N/A' else suffix
    return _exchange_suffix_map.get(exchange, '')


def _store_esg(symbol: str, exchange: str | None) -> None:
    try:
        suffix = _get_exchange_suffix(exchange)
        fmp_symbol = f"{symbol}{suffix}" if suffix else symbol
        disclosure, rating = api_stocks.fetch_esg_data(fmp_symbol)
        esg_qualified, esg_factors = _esg.qualify(disclosure, rating)
        update_esg_data(symbol, esg_qualified, esg_factors)
    except Exception as e:
        log.record_notice(f"Failed to store ESG for '{symbol}': {e}")
