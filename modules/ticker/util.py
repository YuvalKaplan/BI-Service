import re
from modules.core import api_stocks

NAME_NOISE: set[str] = {
    'the', 'a', 'an',
    'inc', 'incorporated', 'corp', 'corporation', 'co', 'company', 'cos',
    'ltd', 'limited', 'llc', 'lp', 'llp',
    'plc', 'ag', 'se', 'sa', 'sas', 'nv', 'bv', 'gmbh', 'spa', 'srl', 'ab',
    'holdings', 'holding', 'group', 'international', 'industries', 'industry',
    'of', 'and', 'de', 'la', 'del',
    'common', 'stock', 'shares', 'share', 'ord', 'pref', 'preferred', 'adr', 'gdr', 'reg',
}

# Pure numbers ("0") and currency/par-value fragments ("krw5000") that providers append to
# security names ("... COMMON STOCK KRW5000.0") and that never appear in FMP company names.
_NAME_NOISE_PATTERN = re.compile(r'^(?:\d+|[a-z]{3}\d+)$')

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
    'CPH': 'DKK', 'HEL': 'EUR', 'BRU': 'EUR', 'LIS': 'EUR', 'DUB': 'EUR',
    'TLV': 'ILS', 'NZE': 'NZD', 'SET': 'THB', 'KLS': 'MYR', 'IST': 'TRY',
    'MEX': 'MXN', 'SAU': 'SAR', 'KOE': 'KRW', 'TWO': 'TWD',
}

# The country an exchange is located in — used to find a company's home-market (primary)
# listing. Secondary venues that only carry other markets' securities (OTC pink sheets,
# LSE's International Order Book, Cboe Europe) are deliberately absent: they're never "home".
EXCHANGE_COUNTRY: dict[str, str] = {
    'NYSE': 'US', 'NASDAQ': 'US', 'AMEX': 'US', 'CBOE': 'US',
    'TSX': 'CA', 'TSXV': 'CA', 'CNQ': 'CA', 'NEO': 'CA',
    'LSE': 'GB',
    'JPX': 'JP',
    'XETRA': 'DE', 'FSX': 'DE', 'BER': 'DE', 'STU': 'DE', 'DUS': 'DE', 'HAM': 'DE', 'MUN': 'DE',
    'PAR': 'FR', 'AMS': 'NL', 'MIL': 'IT', 'VIE': 'AT', 'BME': 'ES', 'BRU': 'BE', 'LIS': 'PT',
    'DUB': 'IE', 'HEL': 'FI', 'CPH': 'DK', 'STO': 'SE', 'OSL': 'NO', 'SIX': 'CH', 'ATH': 'GR',
    'WSE': 'PL', 'PRA': 'CZ', 'IST': 'TR',
    'HKSE': 'HK', 'SHH': 'CN', 'SHZ': 'CN',
    'NSE': 'IN', 'BSE': 'IN',
    'TAI': 'TW', 'TWO': 'TW',
    'KSC': 'KR', 'KOE': 'KR',
    'ASX': 'AU', 'NZE': 'NZ', 'SES': 'SG', 'SET': 'TH', 'KLS': 'MY', 'JKT': 'ID',
    'SAO': 'BR', 'MEX': 'MX', 'SGO': 'CL', 'BUE': 'AR',
    'JNB': 'ZA', 'TLV': 'IL', 'SAU': 'SA', 'DFM': 'AE', 'DOH': 'QA',
}

# Exchanges whose listings make a company "US-listed" for region purposes (OTC excluded:
# it mostly carries foreign companies' unsponsored ADRs).
US_LISTING_EXCHANGES: set[str] = {'NYSE', 'NASDAQ', 'AMEX'}

# Extra countries counted as a company's home market beyond its own (HK-listed Chinese companies).
HOME_COUNTRY_ALIASES: dict[str, set[str]] = {'CN': {'HK'}}


def home_countries(country: str | None) -> set[str]:
    if not country:
        return set()
    return {country} | HOME_COUNTRY_ALIASES.get(country, set())


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
    return [t for t in raw if len(t) > 1 and t not in NAME_NOISE and not _NAME_NOISE_PATTERN.match(t)]


def longest_name_token(name: str) -> str | None:
    """Return the longest meaningful token from a company name."""
    tokens = name_tokens(name)
    return max(tokens, key=len) if tokens else None


def _tokens_match(a: str, b: str) -> bool:
    """Equal, or one is a truncation of the other (providers abbreviate: "ELECTR" -> "Electronics")."""
    if a == b:
        return True
    short, long_ = sorted((a, b), key=len)
    return len(short) >= 4 and long_.startswith(short)


def names_match(holding_name: str, api_name: str) -> bool:
    """
    Return True if the two names refer to the same company after stripping noise words: every
    token of the shorter name must match a token of the longer one, and the matches must cover
    more than half of the longer name. Sharing a word or two is not enough — "Hyundai
    Corporation" must not match "Hyundai Rotem", nor "Grupo Financiero Banorte" match "Grupo
    Financiero Galicia". A loose match here maps unrelated holdings onto one ticker, which then
    shows up as duplicate lines with inconsistent prices.
    """
    a = set(name_tokens(holding_name))
    b = set(name_tokens(api_name))
    if not a or not b:
        return False
    shorter, longer = sorted((a, b), key=len)
    matched = sum(1 for s in shorter if any(_tokens_match(s, l) for l in longer))
    return matched == len(shorter) and matched * 2 > len(longer)


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
        # Search by the cleaned full name first, then by its longest token. Results are only
        # candidates — accept the first whose name actually matches the holding's, never
        # results[0] blindly.
        tokens = name_tokens(name)
        queries = list(dict.fromkeys(q for q in (" ".join(tokens), longest_name_token(name)) if q))
        for query in queries:
            for r in api_stocks.search_by_name(query):
                api_name = r.get('name')
                if not api_name or is_unwanted_names(api_name) or r.get('exchange') == 'CRYPTO':
                    continue
                if names_match(name, api_name):
                    return r.get('symbol') or None

    return None
