import log
import os
import time
from collections import deque
from threading import Lock
from urllib.request import urlopen
import json

API_URL = 'https://financialmodelingprep.com/stable/'

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
        url = (f"{API_URL}/profile?symbol={symbol}&apikey={os.getenv('SECRET_MARKET_DATE_API_KEY')}")
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

