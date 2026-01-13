import log
import os
from urllib.request import urlopen
import json

API_KEY = os.getenv('SECRET_MARKET_DATE_API_KEY')
API_URL = 'https://financialmodelingprep.com/stable/'


def get_jsonparsed_data(url) -> dict:
    response = urlopen(url)
    if response.code != 200:
        raise Exception(f"{response.reason}")
    data = response.read().decode("utf-8")
    return json.loads(data)

def get_stock_profile(symbol: str) -> dict[str,str] | None:
    try:
        url = (f"{API_URL}/profile?symbol={symbol}&apikey={API_KEY}")
        array = get_jsonparsed_data(url)
        return array[0]
    except Exception as e:
        log.record_error(f"Failed to get stock profile for {symbol}. Response from service provider: {e}")
        return None

