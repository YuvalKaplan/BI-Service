import re
import log
from datetime import datetime, date, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from modules.core import api_stocks
from modules.ticker import util as tu
from modules.object.ticker import Ticker, upsert_by_symbol, update_invalid, update_esg_data
from modules.object.ticker_value import TickerValue, upsert as _upsert_tv
from modules.object import categorize_ticker as _cat_ticker
from modules.calc import esg as _esg

_VALUE_DATE_CUT_OFF_HOUR = 17

def populate_esg(ticker_id: int, full_symbol: str) -> None:
    try:
        disclosure, rating = api_stocks.fetch_esg_data(full_symbol)
        esg_qualified, esg_factors = _esg.qualify(disclosure, rating)
        update_esg_data(ticker_id, esg_qualified, esg_factors)
    except Exception as e:
        log.record_notice(f"Failed to store ESG for '{full_symbol}': {e}")


class TickerResolver:

    POPULATE_TICKER          = 'ticker'
    POPULATE_CATEGORY_TICKER = 'category_ticker'

    def __init__(self, populate: str):
        self.populate = populate
        self.style_type: str | None = None
        self.cap_type: str | None = None
        self._symbol_cache: dict[str, Any] = {}
        self._isin_cache:   dict[str, Any] = {}
        self._exchange_suffix_map: dict[str, str] = {}

    def set_classification(self, style_type: str, cap_type: str) -> None:
        self.style_type = style_type
        self.cap_type = cap_type

    def resolve(
        self,
        region: str,
        symbol: str | None,
        isin: str | None = None,
        name: str | None = None,
    ) -> Any:
        if region == 'US':
            return self._resolve_by_symbol(symbol)
        return self._resolve_non_us(symbol, isin, name)

    def get_full_symbol(self, ticker: Ticker) -> str:
        if ticker.exchange and not self._exchange_suffix_map:
            for e in api_stocks.fetch_available_exchanges():
                code = e.get('exchange')
                suffix = e.get('symbolSuffix', '')
                if code:
                    self._exchange_suffix_map[code] = '' if suffix == 'N/A' else suffix
        suffix = self._exchange_suffix_map.get(ticker.exchange, '') if ticker.exchange else ''
        return f"{ticker.symbol}{suffix}" if suffix else ticker.symbol

    def _resolve_by_symbol(self, symbol: str | None) -> Any:
        if not symbol:
            return None
        if symbol in self._symbol_cache:
            return self._symbol_cache[symbol]

        profile = api_stocks.get_stock_profile(symbol)
        if not isinstance(profile, dict):
            log.record_notice(f"No stocks data provider profile for symbol '{symbol}': {profile}")
            self._symbol_cache[symbol] = None
            return None

        result = self._populate(profile)
        self._symbol_cache[symbol] = result
        return result

    def _resolve_non_us(self, symbol: str | None, isin: str | None, name: str | None = None) -> Any:
        """Non-US path: prefer ISIN lookup; fall back to symbol search when ISIN is absent."""
        if isin:
            return self._resolve_by_isin(isin)
        return self._resolve_by_symbol_search(symbol, name)

    def _resolve_by_isin(self, isin: str | None) -> Any:
        if not isin:
            return None
        if isin in self._isin_cache:
            return self._isin_cache[isin]

        search_result = api_stocks.search_by_isin(isin)
        if not search_result:
            log.record_notice(f"No stocks data provider search result for ISIN '{isin}'")
            self._isin_cache[isin] = None
            return None
        symbol_full = search_result.get('symbol')
        if not symbol_full:
            self._isin_cache[isin] = None
            return None
        symbol = re.split(r'[\s.]', symbol_full)[0]

        if symbol in self._symbol_cache:
            result = self._symbol_cache[symbol]
            self._isin_cache[isin] = result
            return result

        profile = api_stocks.get_stock_profile(symbol_full)
        if not isinstance(profile, dict):
            log.record_notice(f"No stocks data provider profile for symbol '{symbol}' (ISIN '{isin}'): {profile}")
            self._isin_cache[isin] = None
            return None

        result = self._populate(profile)
        self._isin_cache[isin] = result
        return result

    def _resolve_by_symbol_search(self, symbol: str | None, name: str | None = None) -> Any:
        """Resolve a non-US ticker that has no ISIN via FMP symbol search."""
        if not symbol:
            return None

        cache_key = symbol
        if cache_key in self._symbol_cache:
            return self._symbol_cache[cache_key]

        query = re.split(r'[\s.]', symbol)[0]
        candidates = api_stocks.search_by_symbol(query)
        matched = tu.filter_symbol_candidates(candidates, query)

        result = None
        for candidate in matched:
            fmp_symbol_full = candidate.get('symbol')
            if not fmp_symbol_full:
                continue
            api_name = candidate.get('name', '')
            if name and api_name and not tu.names_match(name, api_name):
                continue
            profile = api_stocks.get_stock_profile(fmp_symbol_full)
            if not isinstance(profile, dict):
                continue
            result = self._populate(profile)
            break

        if result is None and name:
            fmp_symbol_full = tu.resolve_ticker_from_alt_data(isin=None, name=name)
            if fmp_symbol_full:
                profile = api_stocks.get_stock_profile(fmp_symbol_full)
                if isinstance(profile, dict):
                    result = self._populate(profile)

        if result is None:
            log.record_notice(f"No verified stocks data provider match for non-US symbol '{symbol}'")

        self._symbol_cache[cache_key] = result
        return result

    def _populate(self, profile: dict) -> int | None:
        if self.populate == TickerResolver.POPULATE_CATEGORY_TICKER:
            return self._populate_category_ticker(profile)
        return self._populate_ticker(profile)

    def _populate_ticker(self, profile: dict) -> int | None:
        full_symbol = profile.get('symbol')
        exchange = profile.get('exchange')
        assert(full_symbol)
        assert(exchange)
        bare_symbol = re.split(r'[\s.]', full_symbol)[0]
        is_active = profile.get('isActivelyTrading')
        ticker = Ticker(
            symbol=bare_symbol,
            isin=profile.get('isin'),
            cusip=profile.get('cusip'),
            cik=profile.get('cik'),
            name=profile.get('companyName'),
            exchange=exchange,
            industry=profile.get('industry'),
            sector=profile.get('sector'),
            country=profile.get('country'),
            currency=profile.get('currency'),
            source='fmp',
            is_actively_trading=bool(is_active) if is_active is not None else None,
        )
        ticker_id, is_new = upsert_by_symbol(ticker)

        if profile.get('exchange') == 'CRYPTO':
            update_invalid(ticker_id, 'Crypto')
            return None

        name = profile.get('companyName')
        if not name:
            update_invalid(ticker_id, 'Missing details')
            return None
        if tu.is_unwanted_names(name):
            update_invalid(ticker_id, 'Fund or ETF')
            return None

        self._store_ticker_value(ticker_id, profile)
        
        if is_new:
            populate_esg(ticker_id, full_symbol)
        return ticker_id

    def _populate_category_ticker(self, profile: dict) -> int | None:
        full_symbol = profile.get('symbol')
        if not full_symbol:
            return None
        canonical = re.split(r'[\s.]', full_symbol)[0]
        _, factors = api_stocks.fetch_company_factors(full_symbol)
        if not factors:
            return None
        return _cat_ticker.upsert({
            "name":       profile.get('companyName'),
            "symbol":     canonical,
            "isin":       profile.get('isin'),
            "exchange":   profile.get('exchange'),
            "country":    profile.get('country'),
            "currency":   profile.get('currency'),
            "style_type": self.style_type,
            "cap_type":   self.cap_type,
            "sector":     profile.get('sector'),
            "market_cap": profile.get('marketCap'),
            "factors":    factors,
        })

    def _store_ticker_value(self, ticker_id: int, profile: dict) -> None:
        try:
            price = profile.get('price')
            market_cap = profile.get('marketCap')
            now_et = datetime.now(ZoneInfo("America/New_York"))
            value_date = (now_et - timedelta(days=1) if now_et.hour < _VALUE_DATE_CUT_OFF_HOUR else now_et).date()

            if price and market_cap:
                _upsert_tv(TickerValue(
                    ticker_id=ticker_id,
                    value_date=value_date,
                    stock_price=float(price),
                    market_cap=float(market_cap),
                ))
        except Exception as e:
            log.record_notice(f"Failed to store ticker_value for ticker_id={ticker_id}: {e}")

