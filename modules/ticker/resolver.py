import re
import log
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

from modules.core import api_stocks
from modules.ticker import util as tu
from modules.object.ticker import Ticker, upsert_by_symbol, update_invalid, update_esg_data
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

def populate_esg(ticker_id: int, full_symbol: str) -> None:
    try:
        disclosure, rating = api_stocks.fetch_esg_data(full_symbol)
        esg_qualified, esg_factors = _esg.qualify(disclosure, rating)
        update_esg_data(ticker_id, esg_qualified, esg_factors)
    except Exception as e:
        log.record_notice(f"Failed to store ESG for '{full_symbol}': {e}")

def get_full_symbol(ticker: Ticker) -> str:
    if ticker.exchange and not _exchange_suffix_map:
        for e in api_stocks.fetch_available_exchanges():
            code = e.get('exchange')
            suffix = e.get('symbolSuffix', '')
            if code:
                _exchange_suffix_map[code] = '' if suffix == 'N/A' else suffix
    suffix = _exchange_suffix_map.get(ticker.exchange, '') if ticker.exchange else ''
    return f"{ticker.symbol}{suffix}" if suffix else ticker.symbol

def _get_value_date() -> date:
    now_et = datetime.now(ZoneInfo("America/New_York"))
    return (now_et - timedelta(days=1) if now_et.hour < _VALUE_DATE_CUT_OFF_HOUR else now_et).date()


def _populate(profile: dict) -> int | None:
    """Shared post-profile logic: upsert ticker, validate name, store value + ESG."""
    full_symbol = profile.get('symbol')
    exchange = profile.get('exchange')
    assert(full_symbol)
    assert(exchange)
    bare_symbol = re.split(r'[\s.]', full_symbol)[0]
    ticker = Ticker(
        symbol=bare_symbol,
        isin=profile.get('isin'),
        cusip=profile.get('cusip'),
        cik=profile.get('cik'),
        name=profile.get('companyName'),
        exchange=exchange,
        industry=profile.get('industry'),
        sector=profile.get('sector'),
        currency=profile.get('currency'),
        source='fmp',
    )
    ticker_id, is_new = upsert_by_symbol(ticker)

    if profile.get('exchange') == 'CRYPTO':
        update_invalid(ticker_id, 'Crypto')
        return None

    name = profile.get('companyName')
    if not name:
        update_invalid(ticker_id, 'Missing details')
        return None
    if tu.is_unwanted_names(name):
        update_invalid(ticker_id, 'Fund or ETF')
        return None

    _store_ticker_value(ticker_id, profile)
    if is_new:
        populate_esg(ticker_id, full_symbol)
    return ticker_id


def _resolve_by_symbol(symbol: str | None) -> int | None:
    if not symbol:
        return None

    if symbol in _symbol_cache:
        return _symbol_cache[symbol]

    profile = api_stocks.get_stock_profile(symbol)
    if not isinstance(profile, dict):
        log.record_notice(f"No stocks data provider profile for symbol '{symbol}': {profile}")
        _symbol_cache[symbol] = None
        return None

    result = _populate(profile)
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


    if symbol in _symbol_cache:
        result = _symbol_cache[symbol]
        _isin_cache[isin] = result
        return result

    profile = api_stocks.get_stock_profile(symbol_full)
    if not isinstance(profile, dict):
        log.record_notice(f"No stocks data provider profile for symbol '{symbol}' (ISIN '{isin}'): {profile}")
        _isin_cache[isin] = None
        return None

    result = _populate(profile)
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
        profile = api_stocks.get_stock_profile(fmp_symbol_full)
        if not isinstance(profile, dict):
            continue
        result = _populate(profile)
        break

    if result is None and name:
        fmp_symbol_full = tu.resolve_ticker_from_alt_data(isin=None, name=name)
        if fmp_symbol_full:
            profile = api_stocks.get_stock_profile(fmp_symbol_full)
            if isinstance(profile, dict):
                result = _populate(profile)

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


