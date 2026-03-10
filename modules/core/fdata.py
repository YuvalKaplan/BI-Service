import log
import os
import time
from datetime import date
from collections import deque
from threading import Lock
from urllib.request import urlopen
import json

FMP_API_URL = 'https://financialmodelingprep.com/stable/'
FINNHUB_API_URL = 'https://finnhub.io/api/v1/'


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


def get_jsonparsed_data(url) -> dict:
    response = urlopen(url)
    if response.code != 200:
        raise Exception(f"{response.reason}")
    data = response.read().decode("utf-8")
    return json.loads(data)

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

def get_etf_holdings(symbol: str, on_date: date) -> list[dict[str,str]] | str:
    try:
        throttle_api_calls()
        url = (f"{FINNHUB_API_URL}/etf/holdings?symbol={symbol}&date={on_date.strftime("%Y-%m-%d")}&token={os.getenv('SECRET_HOLDINGS_DATA_API_KEY')}")
        array = get_jsonparsed_data(url)
        
        if not isinstance(array, list) or len(array) == 0:
            message = f"Invalid profile response for symbol '{symbol}': empty result"
            log.record_notice(message) 
            return message
        
        return array[0]['holdings']
    
    except Exception as e:
        message = f"Failed to get ETF holdings for {symbol} on {on_date.strftime("%Y-%m-%d")}. Response from service provider: {e}"
        log.record_error(message)
        return message
