import log
import os
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict
from datetime import date
from collections import deque
from threading import Lock
from urllib.request import urlopen
import json

FMP_API_URL = 'https://financialmodelingprep.com/stable'

CALLS_PER_MINUTE = 200
WINDOW_SECONDS = 60.0

_call_timestamps = deque()
_bucket_lock = Lock()

def throttle_api_calls() -> None:
    """
    Token bucket rate limiter.
    Guarantees no more than CALLS_PER_MINUTE in WINDOW_SECONDS.
    Thread-safe and burst-safe.
    """
    with _bucket_lock:
        now = time.monotonic()

        # Remove timestamps older than the rolling window
        while _call_timestamps and now - _call_timestamps[0] >= WINDOW_SECONDS:
            _call_timestamps.popleft()

        # If bucket is full, wait until a token frees up
        if len(_call_timestamps) >= CALLS_PER_MINUTE:
            sleep_time = WINDOW_SECONDS - (now - _call_timestamps[0])
            if sleep_time > 0:
                time.sleep(sleep_time)

            # Recalculate after sleeping
            now = time.monotonic()
            while _call_timestamps and now - _call_timestamps[0] >= WINDOW_SECONDS:
                _call_timestamps.popleft()

        # Consume a token
        _call_timestamps.append(time.monotonic())

API_RETRIES = 3
API_RETRY_DELAY = 2.0  # seconds; doubles on each subsequent attempt

def get_jsonparsed_data(url: str) -> dict:
    last_exc: Exception | None = None
    for attempt in range(API_RETRIES):
        try:
            response = urlopen(url)
            if response.code != 200:
                raise Exception(f"{response.reason}")
            data = response.read().decode("utf-8")
            return json.loads(data)
        except Exception as e:
            last_exc = e
            if attempt < API_RETRIES - 1:
                time.sleep(API_RETRY_DELAY * (2 ** attempt))
    raise last_exc if last_exc is not None else Exception("Failed to fetch data after retries")

def get_stock_profile(symbol: str) -> dict[str,str] | str:
    try:
        throttle_api_calls()
        url = (f"{FMP_API_URL}/profile?symbol={symbol}&apikey={os.getenv('SECRET_MARKET_DATA_API_KEY')}")
        array = get_jsonparsed_data(url)
        
        if not isinstance(array, list) or len(array) == 0:
            message = f"Invalid profile response for symbol '{symbol}': empty result"
            log.record_notice(message) 
            return message
        
        return array[0]
    
    except Exception as e:
        message = f"Failed to get stock profile for {symbol}. Response from service provider: {e}"
        log.record_error(message)
        return message
    
def get_etf_holdings(symbol: str) -> list[dict]:
    """A fund's full holdings (asset = FMP symbol, isin, securityCusip, marketValue,
    weightPercentage, sharesNumber) — [] on failure. Used for the index funds whose weights are
    float-adjusted caps (modules/ticker/free_float.py)."""
    try:
        throttle_api_calls()
        url = f"{FMP_API_URL}/etf/holdings?symbol={symbol}&apikey={os.getenv('SECRET_MARKET_DATA_API_KEY')}"
        rows = get_jsonparsed_data(url)
        return rows if isinstance(rows, list) else []
    except Exception as e:
        log.record_notice(f"Failed to get ETF holdings for {symbol}: {e}")
        return []

def get_etf_info(symbol: str) -> dict | None:
    """A fund's FMP profile (assetClass, etfCompany, description, website, assetsUnderManagement,
    nav, navCurrency, expenseRatio, inceptionDate, isin, sectorsList - the same weights as
    etf/sector-weightings), or None when FMP has none. Raises when the call fails, so a failure
    isn't taken for a fund FMP doesn't carry."""
    throttle_api_calls()
    url = f"{FMP_API_URL}/etf/info?symbol={symbol}&apikey={os.getenv('SECRET_MARKET_DATA_API_KEY')}"
    rows = get_jsonparsed_data(url)
    if not isinstance(rows, list):
        raise Exception(f"Unexpected FMP etf/info response for {symbol}: {str(rows)[:200]}")
    return rows[0] if rows else None

SHARES_FLOAT_PAGE_LIMIT = 5000

def get_all_shares_float() -> list[dict]:
    """FMP's free float for every listing it has (symbol = FMP full symbol, freeFloat %,
    floatShares, outstandingShares), paged until a short page. Raises when a page fails, so a
    partial list is never taken for the whole."""
    out: list[dict] = []
    page = 0
    while True:
        throttle_api_calls()
        url = (f"{FMP_API_URL}/shares-float-all?page={page}&limit={SHARES_FLOAT_PAGE_LIMIT}"
               f"&apikey={os.getenv('SECRET_MARKET_DATA_API_KEY')}")
        rows = get_jsonparsed_data(url)
        if not isinstance(rows, list):
            raise Exception(f"Unexpected shares-float-all response on page {page}: {str(rows)[:200]}")
        out.extend(rows)
        if len(rows) < SHARES_FLOAT_PAGE_LIMIT:
            return out
        page += 1

def get_quarterly_weighted_shares(symbol: str) -> float | None:
    """The weighted-average share count of the latest quarterly income statement (FMP's
    financials — a source independent of its quote and its market-cap history), or None."""
    try:
        throttle_api_calls()
        url = f"{FMP_API_URL}/income-statement?symbol={symbol}&period=quarter&limit=1&apikey={os.getenv('SECRET_MARKET_DATA_API_KEY')}"
        rows = get_jsonparsed_data(url)
        if not isinstance(rows, list) or not rows:
            return None
        shares = rows[0].get('weightedAverageShsOut')
        return float(shares) if shares else None
    except Exception as e:
        log.record_notice(f"Failed to get quarterly weighted shares for {symbol}: {e}")
        return None

def search_by_isin(isin: str) -> dict | None:
    try:
        throttle_api_calls()
        url = f"{FMP_API_URL}/search-isin?isin={isin}&apikey={os.getenv('SECRET_MARKET_DATA_API_KEY')}"
        result = get_jsonparsed_data(url)
        if not isinstance(result, list) or len(result) == 0:
            return None
        return result[0]
    except Exception as e:
        log.record_notice(f"Failed to search by ISIN '{isin}': {e}")
        return None

def search_by_symbol(query: str) -> list[dict]:
    try:
        throttle_api_calls()
        url = f"{FMP_API_URL}/search-symbol?query={query}&limit=1000&apikey={os.getenv('SECRET_MARKET_DATA_API_KEY')}"
        result = get_jsonparsed_data(url)
        if not isinstance(result, list):
            return []
        return result
    except Exception as e:
        log.record_notice(f"Failed to search by symbol '{query}': {e}")
        return []

def search_by_name(query: str) -> list[dict]:
    try:
        throttle_api_calls()
        url = f"{FMP_API_URL}/search-name?query={query}&apikey={os.getenv('SECRET_MARKET_DATA_API_KEY')}"
        result = get_jsonparsed_data(url)
        if not isinstance(result, list):
            return []
        return result
    except Exception as e:
        log.record_notice(f"Failed to search by name '{query}': {e}")
        return []

def fetch_available_exchanges() -> list[dict]:
    try:
        throttle_api_calls()
        url = f"{FMP_API_URL}/available-exchanges?apikey={os.getenv('SECRET_MARKET_DATA_API_KEY')}"
        result = get_jsonparsed_data(url)
        if not isinstance(result, list):
            return []
        return result
    except Exception as e:
        log.record_notice(f"Failed to fetch available exchanges: {e}")
        return []

def get_symbol_historic_prices(symbol: str, start: date, end: date) -> list[dict[str,str]] | str:
    try:
        throttle_api_calls()
        url = (f"{FMP_API_URL}/historical-price-eod/light?symbol={symbol}&from={start.strftime("%Y-%m-%d")}&to={end.strftime("%Y-%m-%d")}&apikey={os.getenv('SECRET_MARKET_DATA_API_KEY')}")
        array = get_jsonparsed_data(url)
        
        if not isinstance(array, list) or len(array) == 0:
            message = f"Invalid stock price on date response for symbol '{symbol}': empty result"
            log.record_notice(message) 
            return message
        
        return array
    
    except Exception as e:
        message = f"Failed to get stock price on date for {symbol}. Response from service provider: {e}"
        log.record_error(message)
        return message

def get_stock_historic_dividend(symbol: str) -> list[dict[str,str]] | str:
    try:
        throttle_api_calls()
        url = (f"{FMP_API_URL}/dividends?symbol={symbol}&apikey={os.getenv('SECRET_MARKET_DATA_API_KEY')}")
        array = get_jsonparsed_data(url)
        
        if not isinstance(array, list):
            message = f"Invalid historic dividends response for symbol '{symbol}': empty result"
            log.record_notice(message) 
            return message
        
        return array
    
    except Exception as e:
        message = f"Failed to get historic dividends for {symbol}. Response from service provider: {e}"
        log.record_error(message)
        return message

def get_stock_historic_splits(symbol: str) -> list[dict[str,str]] | str:
    try:
        throttle_api_calls()
        url = (f"{FMP_API_URL}/splits?symbol={symbol}&apikey={os.getenv('SECRET_MARKET_DATA_API_KEY')}")
        array = get_jsonparsed_data(url)
        
        if not isinstance(array, list):
            message = f"Invalid historic splits response for symbol '{symbol}': empty result"
            log.record_notice(message) 
            return message
        
        return array
    
    except Exception as e:
        message = f"Failed to get historic splits for {symbol}. Response from service provider: {e}"
        log.record_error(message)
        return message

def get_stock_historic_market_cap(symbol: str, start: date, end: date) -> list[dict[str,str]] | str:
    try:
        throttle_api_calls()
        url = (f"{FMP_API_URL}/historical-market-capitalization?symbol={symbol}&from={start.strftime("%Y-%m-%d")}&to={end.strftime("%Y-%m-%d")}&apikey={os.getenv('SECRET_MARKET_DATA_API_KEY')}")
        array = get_jsonparsed_data(url)
        
        if not isinstance(array, list) or len(array) == 0:
            message = f"Invalid historic market cap price response for symbol '{symbol}': empty result"
            log.record_notice(message) 
            return message
        
        return array
    
    except Exception as e:
        message = f"Failed to get historic market cap price for {symbol}. Response from service provider: {e}"
        log.record_error(message)
        return message

def get_historic_fx_rates(from_currency: str, to_currency: str, start: date, end: date) -> list[dict[str, str]] | str:
    """
    Historical daily FX rate for from_currency->to_currency over [start, end]. FMP serves
    forex pairs (e.g. symbol 'KRWUSD') through the same historical-price-eod/light endpoint
    used for stocks, so this is a thin, clearly-named wrapper rather than a separate
    implementation — callers should use this (not get_fx_rate, which is spot-only) whenever
    converting a value that isn't for literally today, since rates move meaningfully over time
    (e.g. KRWUSD moved ~6% between 2026-01-01 and 2026-09-23).
    """
    return get_symbol_historic_prices(f"{from_currency}{to_currency}", start, end)


def get_fx_rate(from_currency: str, to_currency: str = 'USD') -> float | str:
    """
    Return the spot exchange rate from from_currency to to_currency.
    Uses the FMP quote-short endpoint with a symbol like 'EURUSD'.
    Returns a float (the rate) or a string error message.
    """
    if from_currency == to_currency:
        return 1.0
    symbol = f"{from_currency}{to_currency}"
    try:
        throttle_api_calls()
        url = f"{FMP_API_URL}/quote-short?symbol={symbol}&apikey={os.getenv('SECRET_MARKET_DATA_API_KEY')}"
        array = get_jsonparsed_data(url)
        if not isinstance(array, list) or len(array) == 0:
            message = f"Invalid FX rate response for '{symbol}': empty result"
            log.record_notice(message)
            return message
        return float(array[0]["price"])
    except Exception as e:
        message = f"Failed to get FX rate for {symbol}. Response from service provider: {e}"
        log.record_error(message)
        return message

# ------------------------------------------------------------------
# Fetch company list and factors forr use in classification universe
# ------------------------------------------------------------------
def fetch_company_factors(symbol: str) -> tuple[Dict, Dict]:
    apikey = os.getenv('SECRET_MARKET_DATA_API_KEY')

    def _fetch(endpoint: str):
        throttle_api_calls()
        return get_jsonparsed_data(endpoint)

    try:
        endpoints = {
            "profile": f"{FMP_API_URL}/profile?symbol={symbol}&apikey={apikey}",
            "growth": f"{FMP_API_URL}/financial-growth?symbol={symbol}&apikey={apikey}",
            "ratios": f"{FMP_API_URL}/ratios-ttm?symbol={symbol}&apikey={apikey}",
            "metrics": f"{FMP_API_URL}/key-metrics-ttm?symbol={symbol}&apikey={apikey}",
            "income": f"{FMP_API_URL}/income-statement?symbol={symbol}&limit=1&apikey={apikey}",
            "cashflow": f"{FMP_API_URL}/cash-flow-statement?symbol={symbol}&limit=1&apikey={apikey}",
        }

        results = {}

        with ThreadPoolExecutor(max_workers=6) as executor:
            futures = {name: executor.submit(_fetch, url) for name, url in endpoints.items()}

            for name, future in futures.items():
                results[name] = future.result()

        profile = results["profile"]
        growth = results["growth"]
        ratios = results["ratios"]
        metrics = results["metrics"]
        income = results["income"]
        cashflow = results["cashflow"]

        if not all([growth, ratios, metrics, income, cashflow]):
            return {}, {}
        
        p = profile[0]
        g = growth[0]
        r = ratios[0]
        m = metrics[0]
        i = income[0]
        c = cashflow[0]

        revenue = i.get("revenue", 1)

        profile = {
            "sector": p.get("sector"),
            "isin": p.get("isin"),
            "cik": p.get("cik"),
            "exchange": p.get("exchange"),
            "company_name": p.get("companyName"),
            "industry": p.get("industry"),
            "market_cap": p.get("marketCap", 0),
            "country": p.get("country"),
            "currency": p.get("currency"),
        }

        factors = {
            "sector": p.get("sector"),
            "industry": p.get("industry"),
            "market_cap": p.get("marketCap", 0),

            # Growth factors
            "revenue_growth": g.get("revenueGrowth"),
            "gross_profit_growth": g.get("grossProfitGrowth"),
            "eps_growth": g.get("epsgrowth"),
            "ebitda_growth": g.get("ebitgrowth"),
            "operating_income_growth": g.get("operatingIncomeGrowth"),
            "net_income_growth": g.get("netIncomeGrowth"),
            "asset_growth": g.get("assetGrowth"),
            "fcf_growth": g.get("freeCashFlowGrowth"),
            "gross_margin": r.get("grossProfitMarginTTM"),
            "operating_margin": r.get("operatingProfitMarginTTM"),
            "rd_ratio": i.get("researchAndDevelopmentExpenses", 0) / max(revenue,1),
            "capex_ratio": abs(c.get("capitalExpenditure",0)) / max(revenue,1),

            # Value factors
            "pe": r.get("priceToEarningsRatioTTM"),
            "pb": r.get("priceToBookRatioTTM"),
            "ps": r.get("priceToSalesRatioTTM"),
            "ev_ebitda": m.get("enterpriseValueMultipleTTM"),
            "price_to_fcf": m.get("priceToFreeCashFlowRatioTTM"),
            "price_to_sales": m.get("priceToSalesRatioTTM"),
            "price_to_operating_cf": m.get("priceToOperatingCashFlowRatioTTM"),

            # Yield/other value
            "earnings_yield": m.get("earningsYieldTTM"),
            "fcf_yield": m.get("freeCashFlowYieldTTM"),
            "dividend_yield": r.get("dividendYieldTTM"),
            "book_to_price": 1 / (r.get("priceToBookRatioTTM",1) or 1),
            "sales_to_price": 1 / (r.get("priceToSalesRatioTTM",1) or 1),
            "cashflow_to_price": 1 / (m.get("priceToOperatingCashFlowsRatioTTM",1) or 1)
        }
        return profile, factors
    
    except Exception as e:
        message = f"Failed to get factors for classificationfor symbol {symbol}. Response from service provider: {e}"
        log.record_error(message)
        raise e

# ------------------------------------------------------------------
# Fetch ESG disclosure and risk rating for a single symbol
# ------------------------------------------------------------------
def fetch_esg_data(symbol: str) -> tuple[Dict, Dict]:
    apikey = os.getenv('SECRET_MARKET_DATA_API_KEY')

    def _fetch(endpoint: str):
        throttle_api_calls()
        return get_jsonparsed_data(endpoint)

    try:
        endpoints = {
            "disclosure": f"{FMP_API_URL}/esg-disclosures?symbol={symbol}&apikey={apikey}",
            "rating":     f"{FMP_API_URL}/esg-ratings?symbol={symbol}&apikey={apikey}",
        }
        results = {}
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {name: executor.submit(_fetch, url) for name, url in endpoints.items()}
            for name, future in futures.items():
                results[name] = future.result()

        d = results["disclosure"]
        r = results["rating"]
        disclosure = max(d, key=lambda x: x.get('date', ''), default={}) if isinstance(d, list) and d else {}
        rating     = max(r, key=lambda x: x.get('fiscalYear', ''), default={}) if isinstance(r, list) and r else {}
        return disclosure, rating

    except Exception as e:
        log.record_error(f"Failed to get ESG data for {symbol}: {e}")
        return {}, {}


SCREENER_PAGE_LIMIT = 1000
SCREENER_MAX_PAGES = 20

def fetch_company_screener(market_cap_more_than: int, page: int, limit: int = SCREENER_PAGE_LIMIT, exchange: str | None = None) -> list[dict]:
    """
    Paged call to FMP /stable/company-screener filtered by minimum market cap, optionally
    also filtered to a single exchange. FMP's unfiltered screener results are heavily
    US/Canada-biased and don't meaningfully surface most other exchanges at all (confirmed
    empirically: an unfiltered call returns a handful of XETRA-listed companies, while an
    explicit exchange=XETRA call returns the full ~300-company German large-cap universe) —
    callers that want broad international coverage need to loop this over a list of exchanges.
    Returns the list of company dicts ([] past the last page).
    Expected fields per item: symbol, marketCap, country, exchangeShortName, companyName.

    Raises when the call still fails after get_jsonparsed_data's API_RETRIES attempts, or when
    FMP answers with something other than a list (e.g. an error message). Returning [] instead
    would read as "no more companies on this exchange" and silently build a benchmark with an
    exchange missing or cut off mid-pagination.
    """
    throttle_api_calls()
    apikey = os.getenv('SECRET_MARKET_DATA_API_KEY')
    url = (
        f"{FMP_API_URL}/company-screener"
        f"?marketCapMoreThan={market_cap_more_than}&limit={limit}&page={page}"
        f"&isEtf=false&isFund=false&isActivelyTrading=true"
    )
    if exchange:
        url += f"&exchange={exchange}"
    try:
        result = get_jsonparsed_data(url + f"&apikey={apikey}")
    except Exception as e:
        raise Exception(f"FMP company screener failed after {API_RETRIES} attempts (exchange={exchange}, page={page}): {e}")
    if not isinstance(result, list):
        raise Exception(f"Unexpected FMP company screener response (exchange={exchange}, page={page}): {str(result)[:200]}")
    return result

