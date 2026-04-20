import re
import json
import log
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from psycopg.errors import Error
from psycopg.rows import class_row
from dataclasses import dataclass
from modules.core.db import db_pool_instance

@dataclass
class Ticker:
    id: int | None
    symbol: str
    created_at: datetime | None
    source: str | None
    style_type: str | None
    cap_type: str | None
    type_from: str | None
    isin: str | None
    cusip: str | None
    cik: str | None
    exchange: str | None
    name: str | None
    industry: str | None
    sector: str | None
    currency: str | None
    esg_factors: dict | None
    esg_qualified: bool | None
    invalid: str | None

    def __init__(self, symbol: str, created_at: datetime | None = None,
                 source: str | None = None, style_type: str | None = None, cap_type: str | None = None, type_from: str | None = None,
                 isin: str | None = None, cusip: str | None = None, cik: str | None = None, exchange: str | None = None,
                 name: str | None = None, industry: str | None = None, sector: str | None = None,
                 currency: str | None = None, esg_factors: dict | None = None,
                 esg_qualified: bool | None = None, invalid: str | None = None,
                 id: int | None = None):
        self.id = id
        self.symbol = symbol
        self.created_at = created_at
        self.source = source
        self.style_type = style_type
        self.cap_type = cap_type
        self.type_from = type_from
        self.isin = isin
        self.cusip = cusip
        self.cik = cik
        self.exchange = exchange
        self.name = name
        self.industry = industry
        self.sector = sector
        self.currency = currency
        self.esg_factors = esg_factors
        self.esg_qualified = esg_qualified
        self.invalid = invalid


# ── DB read ──────────────────────────────────────────────────────────────────

def fetch_by_symbol(symbol: str) -> Ticker | None:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Ticker)) as cur:
                cur.execute('SELECT * FROM ticker WHERE symbol = %s;', (symbol,))
                return cur.fetchone()
    except Error as e:
        raise Exception(f"Error fetching the Ticker from the DB: {e}")

def fetch_by_symbols(symbols: list[str]) -> list[Ticker]:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Ticker)) as cur:
                cur.execute("SELECT * FROM ticker WHERE symbol = ANY(%s);", (symbols,))
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error fetching the Ticker list from the DB: {e}")

def fetch_by_ids(ids: list[int]) -> list[Ticker]:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Ticker)) as cur:
                cur.execute("SELECT * FROM ticker WHERE id = ANY(%s);", (ids,))
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error fetching Tickers by ids from the DB: {e}")

def fetch_by_isin_and_symbol(isin: str, symbol: str, exchange: str | None = None) -> Ticker | None:
    """Fetch by ISIN + base symbol (suffix stripped) + optional exchange.
    Handles cross-listed stocks that share an ISIN across exchanges."""
    import re as _re
    base_symbol = _re.split(r'[\s.]', symbol)[0]
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Ticker)) as cur:
                if exchange:
                    cur.execute(
                        'SELECT * FROM ticker WHERE isin = %s AND symbol = %s AND exchange = %s;',
                        (isin, base_symbol, exchange),
                    )
                else:
                    cur.execute(
                        'SELECT * FROM ticker WHERE isin = %s AND symbol = %s;',
                        (isin, base_symbol),
                    )
                return cur.fetchone()
    except Error as e:
        raise Exception(f"Error fetching Ticker by ISIN and symbol from the DB: {e}")


# ── DB write ─────────────────────────────────────────────────────────────────

def upsert_by_symbol(item: Ticker) -> int:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                query = """
                    INSERT INTO ticker (symbol, isin, cusip, cik, name, exchange, industry, sector, currency, source, type_from)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol)
                    DO UPDATE
                    SET isin = EXCLUDED.isin,
                        cusip = EXCLUDED.cusip,
                        cik = EXCLUDED.cik,
                        name = EXCLUDED.name,
                        exchange = EXCLUDED.exchange,
                        industry = EXCLUDED.industry,
                        sector = EXCLUDED.sector,
                        currency = EXCLUDED.currency,
                        source = EXCLUDED.source,
                        type_from = EXCLUDED.type_from
                    RETURNING id;
                """
                cur.execute(query, (item.symbol, item.isin, item.cusip, item.cik, item.name, item.exchange, item.industry, item.sector, item.currency, item.source, item.type_from))
                row = cur.fetchone()
                if row is None:
                    raise Exception("INSERT ... RETURNING id returned no row")
                return row[0]
    except Error as e:
        raise Exception(f"Error upserting ticker by symbol into the DB: {e}")

def update_esg_qualified(symbols: list[str]) -> None:
    if not symbols:
        return
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE ticker
                    SET esg_qualified = TRUE
                    WHERE symbol = ANY(%s::text[]);
                """, (symbols,))
    except Error as e:
        raise Exception(f"Error updating esg_qualified in the DB: {e}")

def update_esg_data(symbol: str, esg_qualified: bool, esg_factors: dict) -> None:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE ticker
                    SET esg_qualified = %s,
                        esg_factors   = %s
                    WHERE symbol = %s;
                """, (esg_qualified, json.dumps(esg_factors), symbol))
    except Error as e:
        raise Exception(f"Error updating esg data for {symbol}: {e}")

def update_invalid(symbol: str, reason: str) -> None:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE ticker SET invalid = %s WHERE symbol = %s;", (reason, symbol))
    except Error as e:
        raise Exception(f"Error updating the Ticker invalid reason into the DB: {e}")

def sanitize() -> None:
    """Mark any ticker table rows with non-standard symbols as invalid."""
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT sanitize_tickers();')
    except Error as e:
        raise Exception(f"Error sanitizing tickers: {e}")

def update_style_from_categorization_etfs() -> None:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE public.ticker t
                    SET style_type = ce.style_type,
                        cap_type   = ce.cap_type,
                        type_from  = 'CAT_ETF'
                    FROM public.categorize_etf_holding ceh
                    JOIN public.categorize_etf ce ON ce.id = ceh.categorize_etf_id
                    WHERE t.id       = ceh.ticker_id
                      AND ce.usage   = 'style'
                      AND ce.style_type IS NOT NULL
                      AND t.invalid  IS NULL;
                """)
    except Error as e:
        raise Exception(f"Error updating ticker style from categorization ETFs: {e}")

def update_style_from_provider_etfs() -> None:
    """Fill style_type for unclassified tickers using provider_etf holdings (value/growth only)."""
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE public.ticker t
                    SET style_type = pe.style_type,
                        type_from  = 'PROVIDER_ETF'
                    FROM public.provider_etf_holding peh
                    JOIN public.provider_etf pe ON pe.id = peh.provider_etf_id
                    WHERE t.id         = peh.ticker_id
                      AND t.style_type IS NULL
                      AND t.invalid    IS NULL
                      AND pe.style_type IN ('value', 'growth');
                """)
    except Error as e:
        raise Exception(f"Error updating ticker style from provider ETFs: {e}")


# ── Resolution ───────────────────────────────────────────────────────────────
# Resolves a holding row to a ticker DB id, fetching the FMP profile on first
# encounter and storing ticker, ticker_value, and ESG data in one pass.
# Per-process caches avoid duplicate API calls for the same symbol/ISIN.

from modules.core import api_stocks
from modules.object.ticker_value import TickerValue, upsert as _upsert_tv
from modules.calc import esg as _esg

_REMOVE_ETFS_AND_FUNDS = r'\b(ETF|fund)\b'
_VALUE_DATE_CUT_OFF_HOUR = 17
_symbol_cache:         dict[str, int | None] = {}
_isin_cache:           dict[str, int | None] = {}
_exchange_suffix_map:  dict[str, str]        = {}  # exchange code → symbolSuffix, loaded once


def upsert_ticker(
    region: str | None,
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

    name = profile.get('companyName')
    if not name:
        update_invalid(symbol, 'Missing details')
        return None
    if re.search(_REMOVE_ETFS_AND_FUNDS, name, re.IGNORECASE):
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
    symbol = search_result.get('symbol')
    if not symbol:
        _isin_cache[isin] = None
        return None
    exchange = search_result.get('stockExchange')

    existing = fetch_by_isin_and_symbol(isin, symbol, exchange)
    if existing and existing.invalid:
        _isin_cache[isin] = None
        return None

    profile = api_stocks.get_stock_profile(symbol)
    if not isinstance(profile, dict):
        log.record_notice(f"No stocks data provider profile for symbol '{symbol}' (ISIN '{isin}'): {profile}")
        result = existing.id if existing else None
        _isin_cache[isin] = result
        return result

    result = _populate(profile, symbol, isin)
    _isin_cache[isin] = result
    return result


_NAME_NOISE = {
    'the', 'a', 'an',
    'inc', 'incorporated', 'corp', 'corporation', 'co', 'company', 'cos',
    'ltd', 'limited', 'llc', 'lp', 'llp',
    'plc', 'ag', 'se', 'sa', 'sas', 'nv', 'bv', 'gmbh', 'spa', 'srl', 'ab',
    'holdings', 'holding', 'group', 'international', 'industries', 'industry',
}

def _name_tokens(name: str) -> list[str]:
    """Return the meaningful lowercase tokens from a company name."""
    raw = re.split(r'[\s.\-,&/()\']', name.lower())
    return [t for t in raw if len(t) > 1 and t not in _NAME_NOISE]

def _names_match(holding_name: str, api_name: str) -> bool:
    """Return True if names share at least one meaningful token, or if comparison is not meaningful.
    Returns True (non-comparable = don't filter) when either name is empty, an ETF/fund, or
    yields no meaningful tokens after stripping noise words and single letters."""
    if not holding_name or not api_name:
        return True
    if re.search(r'\b(etf|fund|trust|index)\b', holding_name, re.IGNORECASE) or \
       re.search(r'\b(etf|fund|trust|index)\b', api_name, re.IGNORECASE):
        return True
    a = set(_name_tokens(holding_name))
    b = set(_name_tokens(api_name))
    if not a or not b:
        return True
    return bool(a & b)


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
    matched = _filter_symbol_candidates(candidates, query)

    result = None
    for candidate in matched:
        fmp_symbol = candidate.get('symbol')
        if not fmp_symbol:
            continue
        if name and not _names_match(name, candidate.get('name', '')):
            continue
        profile = api_stocks.get_stock_profile(fmp_symbol)
        if not isinstance(profile, dict):
            continue
        result = _populate(profile, fmp_symbol, None)
        break

    if result is None:
        log.record_notice(f"No verified stocks data provider match for non-US symbol '{symbol}'")

    _symbol_cache[cache_key] = result
    return result

def _filter_symbol_candidates(results: list[dict], query: str) -> list[dict]:
    """Keep results where the FMP symbol exactly matches query or is query.<exchange-letters>.
    Exact matches (no suffix) are returned first, followed by suffixed matches in original order."""
    exact = []
    suffixed = []
    for r in results:
        s = r.get('symbol', '')
        if s == query:
            exact.append(r)
        elif s.startswith(query + '.') and s[len(query) + 1:].isalpha():
            suffixed.append(r)
    return exact + suffixed

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
