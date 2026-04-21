import re
import pandas as pd
from modules.core import api_stocks

NAME_NOISE: set[str] = {
    'the', 'a', 'an',
    'inc', 'incorporated', 'corp', 'corporation', 'co', 'company', 'cos',
    'ltd', 'limited', 'llc', 'lp', 'llp',
    'plc', 'ag', 'se', 'sa', 'sas', 'nv', 'bv', 'gmbh', 'spa', 'srl', 'ab',
    'holdings', 'holding', 'group', 'international', 'industries', 'industry',
}

_ETF_FUND_PATTERN = re.compile(r'\b(?:etf|fund|trust|index)\b', re.IGNORECASE)


def is_etf_or_fund(name: str | None) -> bool:
    return bool(name and _ETF_FUND_PATTERN.search(name))

EXCLUDED_TICKERS: set[str] = {'USD', 'CAD', 'EUR', 'ISR', 'JPY', 'GBP', 'TICKER'}

TREASURY_SECURITIES: set[str] = {
    'XTSLA', 'AGPXX', 'BOXX', 'CMQXX', 'DTRXX', 'FGXXX',
    'FTIXX', 'GVMXX', 'JIMXX', 'JTSXX', 'MGMXX', 'PGLBB', 'SALXX',
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
    """Return True if names share at least one meaningful token, or if comparison is not meaningful.
    Returns True (non-comparable = don't filter) when either name is empty or
    yields no meaningful tokens after stripping noise words and single letters."""
    if not holding_name or not api_name:
        return True
    a = set(name_tokens(holding_name))
    b = set(name_tokens(api_name))
    if not a or not b:
        return True
    return bool(a & b)


def filter_symbol_candidates(results: list[dict], query: str) -> list[dict]:
    """Keep FMP search results where the symbol exactly matches query or is query.<exchange-suffix>,
    and whose name is not an ETF, fund, trust, or index. Exact matches returned first."""
    exact = []
    suffixed = []
    for r in results:
        if is_etf_or_fund(r.get('name')):
            continue
        s = r.get('symbol', '')
        if s == query:
            exact.append(r)
        elif s.startswith(query + '.') and s[len(query) + 1:].isalpha():
            suffixed.append(r)
    return exact + suffixed


def normalize_ticker_series(series: pd.Series) -> pd.Series:
    normalized = (
        series
        .astype(str)
        .str.strip()
        .str.lstrip("'")
        .str.split().str[0]
        .str.split(r'[.\-_]').str[0]
    )
    short_numeric = normalized.str.match(r'^\d{1,3}$')
    return normalized.where(~short_numeric, normalized.str.zfill(4))


def filter_ticker_df(df: pd.DataFrame, col: str, remove_tickers: list[str]) -> pd.DataFrame:
    return df[
        df[col].str.fullmatch(r'[A-Z0-9]+', na=False) &
        ~df[col].isin(EXCLUDED_TICKERS) &
        ~df[col].isin(remove_tickers)
    ]


def is_valid_holding(ticker: str | None, name: str | None) -> bool:
    """Return True if a holding row should be kept (not an option, treasury, ETF, or fund).
    Must be called on raw ticker values before normalization."""
    raw = str(ticker).strip() if ticker else ''
    root = re.split(r'[.\-_]', re.split(r'\s', raw)[0])[0]
    if re.match(r'^\S+\s+\d{6}[CP]\d+', raw):
        return False
    if root in TREASURY_SECURITIES:
        return False
    if is_etf_or_fund(name):
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
