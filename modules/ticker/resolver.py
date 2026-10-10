import re
import log
from datetime import date, timedelta
from typing import Any

from modules.core import api_stocks
from modules.ticker import esg
from modules.ticker import util as tu
from modules.object.ticker import Ticker, upsert_by_symbol, update_invalid
from modules.object import categorize_ticker as _cat_ticker

# Why a ticker is invalid, from its FMP profile (profile_invalid_reason)
CRYPTO, MISSING_DETAILS, ETF, FUND, NOT_TRADING = 'Crypto', 'Missing details', 'ETF', 'Fund', 'Not actively trading'
FUND_REASONS = (ETF, FUND)   # a holding line on one of these tickers isn't a stock (index_funds.is_fund_line)
TRADING_LOOKBACK_DAYS = 10   # a day with volume within this many days keeps a ticker FMP calls inactive valid


def trades_recently(full_symbol: str, today: date | None = None) -> bool:
    """Whether FMP's daily prices show the symbol traded (volume > 0) within TRADING_LOOKBACK_DAYS -
    a second opinion on FMP's isActivelyTrading flag, which is wrong for some (Energy Transfer,
    Dillard's: flagged inactive while trading millions of shares a day). A frozen quote FMP keeps
    serving after a delisting has no volume."""
    today = today or date.today()
    rows = api_stocks.get_symbol_historic_prices(full_symbol, today - timedelta(days=TRADING_LOOKBACK_DAYS), today)
    return isinstance(rows, list) and any((r.get('volume') or 0) > 0 for r in rows)


def profile_invalid_reason(profile: dict, full_symbol: str) -> str | None:
    """Why the listing an FMP profile describes isn't a company's stock we use, or None. A fund or
    ETF by FMP's own flags (isEtf / isFund) - never by its name: Northern Trust, Digital Realty
    Trust and Nippon Building Fund are companies. Not actively trading only when FMP's daily
    prices agree (trades_recently - one more call, only for the listings FMP calls inactive)."""
    if profile.get('exchange') == 'CRYPTO':
        return CRYPTO
    if not profile.get('companyName'):
        return MISSING_DETAILS
    if profile.get('isEtf'):
        return ETF
    if profile.get('isFund'):
        return FUND
    is_active = profile.get('isActivelyTrading')
    if is_active is not None and not is_active and not trades_recently(full_symbol):
        return NOT_TRADING
    return None


class TickerResolver:

    POPULATE_TICKER          = 'ticker'
    POPULATE_CATEGORY_TICKER = 'category_ticker'

    def __init__(self, populate: str):
        self.populate = populate
        self.style_type: str | None = None
        self.cap_type: str | None = None
        self._symbol_cache: dict[str, Any] = {}
        self._isin_cache:   dict[str, Any] = {}
        self._full_symbol_cache: dict[str, Any] = {}
        self._profile_cache: dict[str, dict | None] = {}
        self._name_cache: dict[str, Any] = {}
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

    def resolve_fmp_line(
        self,
        symbol: str | None,
        isin: str | None = None,
        cusip: str | None = None,
        name: str | None = None,
    ) -> Any:
        """
        A line of an FMP fund holdings answer (etf/holdings: asset, isin, securityCusip, name).
        Its symbol is already FMP's, so its profile is used when it is the line's security - the
        ISIN (else the CUSIP) agrees, or with neither on both sides the names match (a fund's
        symbol can be another company's: FAB, First Abu Dhabi Bank, is a First Trust fund on
        FMP). Else the ISIN is searched, else the name (a verified name search - FMP lists some
        stocks by name only, e.g. "SAMSUNG ELECTRONICS CO").
        """
        if symbol:
            profile = self._fmp_profile(symbol)
            if profile is not None and self._same_security(profile, isin, cusip, name):
                if symbol not in self._full_symbol_cache:
                    self._full_symbol_cache[symbol] = self._populate(profile)
                return self._full_symbol_cache[symbol]
        if isin:
            return self._resolve_by_isin(isin)
        if name:
            return self._resolve_by_name(name)
        return None

    def _fmp_profile(self, symbol: str) -> dict | None:
        if symbol not in self._profile_cache:
            profile = api_stocks.get_stock_profile(symbol)
            self._profile_cache[symbol] = profile if isinstance(profile, dict) else None
        return self._profile_cache[symbol]

    @staticmethod
    def _same_security(profile: dict, isin: str | None, cusip: str | None, name: str | None) -> bool:
        if isin and profile.get('isin'):
            return profile['isin'] == isin
        if cusip and profile.get('cusip'):
            return profile['cusip'] == cusip
        return not name or not profile.get('companyName') or tu.names_match(name, profile['companyName'])

    def _resolve_by_name(self, name: str) -> Any:
        if name in self._name_cache:
            return self._name_cache[name]
        result = None
        fmp_symbol_full = tu.resolve_ticker_from_alt_data(isin=None, name=name)
        if fmp_symbol_full:
            profile = self._fmp_profile(fmp_symbol_full)
            if profile is not None:
                result = self._populate(profile)
        self._name_cache[name] = result
        return result

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

        # Keyed by the full FMP symbol (with exchange suffix), not the bare code: a bare code is
        # shared by unrelated listings on different exchanges, and _symbol_cache is keyed by raw
        # provider symbols, so reusing it here could hand back another security's ticker_id.
        if symbol_full in self._full_symbol_cache:
            result = self._full_symbol_cache[symbol_full]
            self._isin_cache[isin] = result
            return result

        profile = api_stocks.get_stock_profile(symbol_full)
        if not isinstance(profile, dict):
            log.record_notice(f"No stocks data provider profile for symbol '{symbol_full}' (ISIN '{isin}'): {profile}")
            self._isin_cache[isin] = None
            self._full_symbol_cache[symbol_full] = None
            return None

        result = self._populate(profile)
        self._isin_cache[isin] = result
        self._full_symbol_cache[symbol_full] = result
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
            average_turnover=tu.profile_turnover(profile),
        )
        ticker_id, is_new = upsert_by_symbol(ticker)

        # Same rule as the profile refresh, applied at once: a symbol FMP stopped trading (NZYM-B
        # after its change to NSIS-B) otherwise stays valid for up to a week, and FMP may keep
        # serving it a frozen quote meanwhile.
        reason = profile_invalid_reason(profile, full_symbol)
        if reason:
            update_invalid(ticker_id, reason)
            return None

        market_cap = profile.get('marketCap')
        if not market_cap:
            update_invalid(ticker_id, 'Missing market cap')
            return None

        # Its price and market cap are stored by the valuation pass (modules/ticker/valuation.py),
        # once per ticker, not here — a ticker held by several providers' ETFs is resolved by each.
        if is_new:
            esg.populate_esg(ticker_id, full_symbol)
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

