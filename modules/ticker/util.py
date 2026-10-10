import re
import unicodedata
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

# FMP search results (symbol / name search) that are obviously not a company, skipped as
# candidates so a search doesn't settle on one. Only a pre-filter: FMP's search answers carry no
# fund flags, the profile decides (resolver.profile_invalid_reason). Never "trust" or "fund" -
# Northern Trust, CapitaLand Integrated Commercial Trust and Nippon Building Fund are companies.
UNWANTED_NAMES = re.compile(r'\b(?:etfs?|indexes?|indices|cryptos?)\b', re.IGNORECASE)


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

# Dutch and Luxembourg holding companies are often primarily listed on another continental
# exchange (Airbus and Euronext in Paris, Stellantis and Ferrari in Milan, argenx in Brussels,
# ArcelorMittal in Amsterdam/Madrid, Tenaris in Milan), so those markets count as home for
# them too — ranked after the domicile country's own exchanges.
WIDER_HOME_COUNTRIES: dict[str, set[str]] = {
    'NL': {'FR', 'BE', 'IT', 'PT', 'ES'},
    'LU': {'NL', 'FR', 'BE', 'IT', 'PT', 'ES'},
}

# LSE's International Order Book: foreign securities mirrored on LSE under 4-character codes
# starting with 0 (0Q16 = Bank of America), carrying the issuer's home-market data. FMP files
# them under 'LSE' or 'IOB'. Never a home listing.
_IOB_SYMBOL = re.compile(r'^0[0-9A-Z]{3}$')


def home_countries(country: str | None) -> set[str]:
    if not country:
        return set()
    return {country} | HOME_COUNTRY_ALIASES.get(country, set())


def is_iob_line(symbol: str | None, exchange: str | None) -> bool:
    return exchange == 'IOB' or (exchange == 'LSE' and bool(symbol) and bool(_IOB_SYMBOL.match(symbol)))


def listing_country(symbol: str | None, exchange: str | None) -> str | None:
    """The country a listing trades in, or None for venues with no country of their own (OTC,
    LSE's International Order Book, unknown exchanges)."""
    if is_iob_line(symbol, exchange):
        return None
    return EXCHANGE_COUNTRY.get(exchange or '')


def market_tier(domicile: str | None, symbol: str | None, exchange: str | None) -> int:
    """How strong a candidate for a company's primary listing a listing is, by market:
    0 = the domicile country's own market (incl. HOME_COUNTRY_ALIASES), 1 = a wider home market
    (WIDER_HOME_COUNTRIES), 2 = a US exchange, 3 = another country's exchange, 4 = a venue with
    no country of its own (OTC, LSE's International Order Book, unknown)."""
    country = listing_country(symbol, exchange)
    if country and country in home_countries(domicile):
        return 0
    if country and country in WIDER_HOME_COUNTRIES.get(domicile or '', set()):
        return 1
    if exchange in US_LISTING_EXCHANGES and not is_iob_line(symbol, exchange):
        return 2
    return 3 if country else 4


def has_screened_home(domicile: str | None, screened_exchanges: list[str] | set[str]) -> bool:
    """Whether any of `screened_exchanges` is in the domicile's home market (tier 0 or 1) — if
    not (Bermuda, Cayman, Jersey, Hungary, Kazakhstan, …), a company domiciled there can only be
    found on a foreign exchange, which then counts as home for it."""
    return any(market_tier(domicile, None, ex) <= 1 for ex in screened_exchanges)


def currency_for_exchange(exchange: str | None) -> str | None:
    """Best-known currency for `exchange`, or None if the exchange isn't in EXCHANGE_CURRENCY
    (caller should then fall back to whatever ticker.currency has, if anything)."""
    return EXCHANGE_CURRENCY.get(exchange) if exchange else None


# FMP quotes some markets in minor units (GBp pence, ZAc cents, ILA agorot) but reports their
# market caps in the major unit.
_MINOR_CURRENCY_UNITS: dict[str, str] = {'GBp': 'GBP', 'GBX': 'GBP', 'ZAc': 'ZAR', 'ZAC': 'ZAR', 'ILA': 'ILS'}


def normalize_currency(currency: str | None) -> str | None:
    return _MINOR_CURRENCY_UNITS.get(currency, currency) if currency else None


def listing_currency(exchange: str | None, currency: str | None = None) -> str | None:
    """The currency a listing's market data is reported in: FMP's own currency for the listing
    (ticker.currency / the profile's `currency`) when known — a market doesn't always report in
    its exchange's currency (Compass on LSE in USD, Jardine Matheson on SES in USD, Hong Kong's
    RMB counters in CNY, LSE's mirrored foreign lines in the issuer's currency) — else the
    exchange's currency."""
    return normalize_currency(currency) or currency_for_exchange(exchange)


def minor_unit_factor(currency: str | None) -> int:
    """100 for a currency FMP quotes in minor units (GBp, ZAc, ILA), else 1."""
    return 100 if currency in _MINOR_CURRENCY_UNITS else 1


def profile_turnover(profile: dict) -> float | None:
    """A listing's average daily turnover from its FMP profile — averageVolume x price, in the
    major unit of its quote currency (a pence price is divided by 100) — or None when missing."""
    volume, price = profile.get('averageVolume'), profile.get('price')
    if volume is None or not price or price <= 0 or volume < 0:
        return None
    return volume * price / minor_unit_factor(profile.get('currency'))

def fold_accents(text: str) -> str:
    """'Telefónica' -> 'Telefonica': FMP spells the same company with and without accents."""
    return ''.join(c for c in unicodedata.normalize('NFKD', text) if not unicodedata.combining(c))


def name_key(name: str | None) -> str:
    """Exact-name identity key: case, accents, punctuation and spacing ignored ("SK Telecom
    Co.,Ltd" == "SK Telecom Co., Ltd."), legal suffixes kept ("Midea Group" != "Midea Group Co.,
    Ltd.") — stripping them could merge unrelated companies."""
    return re.sub(r'[^a-z0-9]', '', fold_accents(name or '').lower())


def name_tokens(name: str) -> list[str]:
    """Return meaningful lowercase tokens from a company name (accents folded), stripping noise words and single chars."""
    raw = re.split(r'[\s.\-,&/()\']', fold_accents(name).lower())
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
    and whose name is not an ETF or index (is_unwanted_names). Exact matches returned first."""
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
