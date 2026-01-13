import os
from urllib.request import urlopen
import json

API_KEY = os.getenv('SECRET_MARKET_DATE_API_KEY')
API_URL = 'https://financialmodelingprep.com/stable/'


def get_jsonparsed_data(url):
    response = urlopen(url)
    data = response.read().decode("utf-8")
    return json.loads(data)

def get_stock_profile(symbol: str):
    url = (f"{API_URL}/profile?symbol={symbol}&apikey={API_KEY}")
    return get_jsonparsed_data(url)

