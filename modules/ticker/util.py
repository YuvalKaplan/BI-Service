import re
from modules.core import api_stocks

NAME_NOISE: set[str] = {
    'the', 'a', 'an',
    'inc', 'incorporated', 'corp', 'corporation', 'co', 'company', 'cos',
    'ltd', 'limited', 'llc', 'lp', 'llp',
    'plc', 'ag', 'se', 'sa', 'sas', 'nv', 'bv', 'gmbh', 'spa', 'srl', 'ab',
    'holdings', 'holding', 'group', 'international', 'industries', 'industry',
}

UNWANTED_NAMES = re.compile(r'\b(?:etfs?|funds?|trusts?|indexes?|indices|cryptos?)\b', re.IGNORECASE)


def is_unwanted_names(name: str | None) -> bool:
    return bool(name and UNWANTED_NAMES.search(name))


# The currency a given exchange quotes prices/market caps in. FMP's own exchange-list endpoint
# doesn't expose this, so it's hardcoded here (exchange->currency is a fixed, well-known fact).
# Keyed by exchange rather than ticker.currency deliberately: a brand-new ticker's `currency`
# field isn't populated until its first profile refresh (a week+ after discovery), but its
# exchange is known immediately from the screener/holdings row, so this avoids a window where
# newly-discovered non-USD tickers would have no way to know their own currency yet.
EXCHANGE_CURRENCY: dict[str, str] = {
    'NYSE': 'USD', 'NASDAQ': 'USD', 'AMEX': 'USD', 'OTC': 'USD',
    'TSX': 'CAD',
    'LSE': 'GBP',
    'JPX': 'JPY',
    'XETRA': 'EUR', 'FSX': 'EUR', 'PAR': 'EUR', 'AMS': 'EUR', 'MIL': 'EUR', 'VIE': 'EUR', 'BME': 'EUR',
    'HKSE': 'HKD',
    'ASX': 'AUD',
    'SIX': 'CHF',
    'SHH': 'CNY', 'SHZ': 'CNY',
    'NSE': 'INR', 'BSE': 'INR',
    'TAI': 'TWD',
    'STO': 'SEK',
    'KSC': 'KRW',
    'SAO': 'BRL',
    'WSE': 'PLN',
    'SES': 'SGD',
    'OSL': 'NOK',
    'JNB': 'ZAR',
    'JKT': 'IDR',
}


def currency_for_exchange(exchange: str | None) -> str | None:
    """Best-known currency for `exchange`, or None if the exchange isn't in EXCHANGE_CURRENCY
    (caller should then fall back to whatever ticker.currency has, if anything)."""
    return EXCHANGE_CURRENCY.get(exchange) if exchange else None

# Currencies and combination holdings (BRK - Berkshire Hathaway)
EXCLUDED_TICKERS: set[str] = {'USD', 'BACKUSD', 'CAD', 'EUR', 'ISR', 'JPY', 'GBP', 'TICKER', 'BRK'}

TREASURY_SECURITIES: set[str] = {
    'XTSLA', 'AGPXX', 'BOXX', 'CMQXX', 'DTRXX', 'FGXXX',
    'FTIXX', 'GVMXX', 'JIMXX', 'JTSXX', 'MGMXX', 'PGLXX', 'PGLBB', 'SALXX',
}


def name_tokens(name: str) -> list[str]:
    """Return meaningful lowercase tokens from a company name, stripping noise words and single chars."""
    raw = re.split(r'[\s.\-,&/()\']', name.lower())
    return [t for t in raw if len(t) > 1 and t not in NAME_NOISE]


def longest_name_token(name: str) -> str | None:
    """Return the longest meaningful token from a company name."""
    tokens = name_tokens(name)
    return max(tokens, key=len) if tokens else None


def names_match(holding_name: str, api_name: str) -> bool:
    """Return True if names share at least one meaningful token after stripping noise words."""
    a = set(name_tokens(holding_name))
    b = set(name_tokens(api_name))
    if not a or not b:
        return False
    return bool(a & b)


def filter_symbol_candidates(results: list[dict], query: str) -> list[dict]:
    """Keep FMP search results where the symbol exactly matches query or is query.<exchange-suffix>,
    and whose name is not an ETF, fund, trust, or index. Exact matches returned first."""
    exact = []
    suffixed = []
    for r in results:
        if is_unwanted_names(r.get('name')):
            continue
        if r.get('exchange') == 'CRYPTO':
            continue
        s = r.get('symbol', '')
        if s == query:
            exact.append(r)
        elif s.startswith(query + '.') and s[len(query) + 1:].isalpha():
            suffixed.append(r)
    return exact + suffixed


def normalize_ticker(ticker: str | None) -> str:
    s = str(ticker).strip().lstrip("'") if ticker else ''
    s = s.split()[0] if s else ''
    s = re.split(r'[.\-_]', s)[0] if s else ''
    if re.match(r'^\d{1,3}$', s):
        s = s.zfill(4)
    return s


def is_included_ticker(ticker: str | None, remove_tickers: list[str]) -> bool:
    if not ticker or not re.fullmatch(r'[A-Z0-9]+', ticker):
        return False
    return ticker not in EXCLUDED_TICKERS and ticker not in remove_tickers


def is_valid_holding(ticker: str | None, name: str | None) -> bool:
    """Return True if a holding row should be kept (not an option, treasury, ETF, or fund).
    Must be called on raw ticker values before normalization."""

    raw = str(ticker).strip() if ticker else ''
    root = re.split(r'[.\-_]', re.split(r'\s', raw)[0])[0]
    if re.match(r'^\S+\s+\d{6}[CP]\d+', raw):
        return False
    if root in TREASURY_SECURITIES:
        return False
    if is_unwanted_names(name):
        return False
    return True


def resolve_ticker_from_alt_data(isin: str | None, name: str | None) -> str | None:
    """Resolve a ticker symbol via ISIN search, then name search. Returns symbol or None."""
    if isin:
        result = api_stocks.search_by_isin(isin)
        if result:
            return result.get('symbol') or None

    if name:
        token = longest_name_token(name)
        if token:
            results = api_stocks.search_by_name(token)
            if results:
                return results[0].get('symbol') or None

    return None
