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

FMP_API_URL = 'https://financialmodelingprep.com/stable/'

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

def get_jsonparsed_data(url) -> dict:
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
        log.record_status("UUUUUUUUUUUUUURRRRRRRRRRRRRRLLLLLLLLLLLLLL")
        log.record_status(url)
        log.record_status("KKKKKKKKKKKKKKKKKKKKKKK")
        log.record_status(f"{os.getenv('SECRET_MARKET_DATA_API_KEY')}")
        log.record_status("--------------------------------------")

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
    
def get_stock_historic_prices(symbol: str, start: date, end: date) -> list[dict[str,str]] | str:
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

# ------------------------------------------------------------------
# Fetch company list and factors forr use in classification universe
# ------------------------------------------------------------------
def fetch_company_factors(symbol) -> tuple[Dict, Dict]:
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

