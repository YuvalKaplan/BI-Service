import json
from datetime import datetime
from typing import Optional
from psycopg.errors import Error
from psycopg.rows import class_row
from dataclasses import dataclass
from modules.core.db import db_pool_instance

@dataclass
class Ticker:
    symbol: str
    id: Optional[int] = None
    created_at: Optional[datetime] = None
    source: str | None = None
    style_type: str | None = None
    cap_type: str | None = None
    type_from: str | None = None
    isin: str | None = None
    cusip: str | None = None
    cik: str | None = None
    exchange: str | None = None
    name: str | None = None
    industry: str | None = None
    sector: str | None = None
    country: str | None = None
    currency: str | None = None
    esg_factors: dict | None = None
    esg_qualified: bool | None = None
    is_actively_trading: bool | None = None
    invalid: str | None = None


# ── DB read ──────────────────────────────────────────────────────────────────

def fetch_by_symbol(symbol: str) -> Ticker | None:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Ticker)) as cur:
                cur.execute('SELECT * FROM ticker WHERE symbol = %s;', (symbol,))
                return cur.fetchone()
    except Error as e:
        raise Exception(f"Error fetching the Ticker from the DB: {e}")

def fetch_all_for_symbol_cache() -> dict[str, int | None]:
    """Return {symbol: id} for valid tickers and {symbol: None} for invalid ones."""
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT symbol, id, invalid FROM ticker;')
                return {row[0]: None if row[2] else row[1] for row in cur.fetchall()}
    except Error as e:
        raise Exception(f"Error fetching all tickers for cache: {e}")

def fetch_all_for_isin_cache() -> dict[str, int | None]:
    """Return {isin: id} for valid tickers and {isin: None} for invalid ones."""
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT isin, id, invalid FROM ticker WHERE isin IS NOT NULL;')
                return {row[0]: None if row[2] else row[1] for row in cur.fetchall()}
    except Error as e:
        raise Exception(f"Error fetching all ISINs for cache: {e}")

def fetch_by_symbols(symbols: list[str]) -> list[Ticker]:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Ticker)) as cur:
                cur.execute("SELECT * FROM ticker WHERE symbol = ANY(%s);", (symbols,))
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error fetching the Ticker list from the DB: {e}")

def fetch_all_valid() -> list['Ticker']:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Ticker)) as cur:
                cur.execute("SELECT * FROM ticker WHERE invalid IS NULL;")
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error fetching all valid tickers: {e}")


def fetch_by_ids(ids: list[int]) -> list[Ticker]:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Ticker)) as cur:
                cur.execute("SELECT * FROM ticker WHERE id = ANY(%s);", (ids,))
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error fetching Tickers by ids from the DB: {e}")

def fetch_by_isin_and_symbol(isin: str, symbol: str, exchange: str | None = None) -> Ticker | None:
    """Fetch by ISIN + base symbol (suffix stripped) + optional exchange.
    Handles cross-listed stocks that share an ISIN across exchanges."""
    import re as _re
    base_symbol = _re.split(r'[\s.]', symbol)[0]
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Ticker)) as cur:
                if exchange:
                    cur.execute(
                        'SELECT * FROM ticker WHERE isin = %s AND symbol = %s AND exchange = %s;',
                        (isin, base_symbol, exchange),
                    )
                else:
                    cur.execute(
                        'SELECT * FROM ticker WHERE isin = %s AND symbol = %s;',
                        (isin, base_symbol),
                    )
                return cur.fetchone()
    except Error as e:
        raise Exception(f"Error fetching Ticker by ISIN and symbol from the DB: {e}")


# ── DB write ─────────────────────────────────────────────────────────────────

def upsert_by_symbol(item: Ticker) -> tuple[int, bool]:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                query = """
                    INSERT INTO ticker (symbol, isin, cusip, cik, name, exchange, industry, sector, country, currency, source, type_from, is_actively_trading)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol, exchange)
                    DO UPDATE
                    SET isin = EXCLUDED.isin,
                        cusip = EXCLUDED.cusip,
                        cik = EXCLUDED.cik,
                        name = EXCLUDED.name,
                        exchange = EXCLUDED.exchange,
                        industry = EXCLUDED.industry,
                        sector = EXCLUDED.sector,
                        country = EXCLUDED.country,
                        currency = EXCLUDED.currency,
                        source = EXCLUDED.source,
                        type_from = EXCLUDED.type_from,
                        is_actively_trading = EXCLUDED.is_actively_trading
                    RETURNING id, (xmax = 0) AS is_new;
                """
                cur.execute(query, (item.symbol, item.isin, item.cusip, item.cik, item.name, item.exchange, item.industry, item.sector, item.country, item.currency, item.source, item.type_from, item.is_actively_trading))
                row = cur.fetchone()
                if row is None:
                    raise Exception("INSERT ... RETURNING id returned no row")
                return row[0], row[1]
    except Error as e:
        raise Exception(f"Error upserting ticker by symbol into the DB: {e}")

def update_profile(ticker_id: int, item: Ticker) -> None:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE ticker
                    SET isin      = %s,
                        cusip     = %s,
                        cik       = %s,
                        name      = %s,
                        exchange  = %s,
                        industry  = %s,
                        sector    = %s,
                        country   = %s,
                        currency  = %s,
                        source    = %s,
                        type_from = %s
                    WHERE id = %s;
                """, (item.isin, item.cusip, item.cik, item.name, item.exchange,
                      item.industry, item.sector, item.country, item.currency, item.source, item.type_from,
                      ticker_id))
    except Error as e:
        raise Exception(f"Error updating profile for ticker {ticker_id}: {e}")


def update_esg_qualified(symbols: list[str]) -> None:
    if not symbols:
        return
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE ticker
                    SET esg_qualified = TRUE
                    WHERE symbol = ANY(%s::text[]);
                """, (symbols,))
    except Error as e:
        raise Exception(f"Error updating esg_qualified in the DB: {e}")

def update_esg_data(ticker_id: int, esg_qualified: bool, esg_factors: dict) -> None:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE ticker
                    SET esg_qualified = %s,
                        esg_factors   = %s
                    WHERE id = %s;
                """, (esg_qualified, json.dumps(esg_factors), ticker_id))
    except Error as e:
        raise Exception(f"Error updating esg data for ticker {ticker_id}: {e}")

def fetch_with_missing_exchange() -> list['Ticker']:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Ticker)) as cur:
                cur.execute("SELECT * FROM ticker WHERE exchange IS NULL AND invalid IS NULL;")
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error fetching tickers with missing exchange: {e}")


def update_invalid(ticker_id: int, reason: str) -> None:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE ticker SET invalid = %s WHERE id = %s;", (reason, ticker_id))
    except Error as e:
        raise Exception(f"Error updating the Ticker invalid reason into the DB: {e}")


def update_style_from_categorization_etfs() -> None:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE public.ticker t
                    SET style_type = ct.style_type,
                        cap_type   = ct.cap_type,
                        type_from  = 'CAT_ETF'
                    FROM public.categorize_ticker ct
                    WHERE t.symbol        = ct.symbol
                      AND ct.style_type IS NOT NULL
                      AND t.invalid     IS NULL;
                """)
    except Error as e:
        raise Exception(f"Error updating ticker style from categorization ETFs: {e}")

def update_style_for_unclassified() -> None:
    """Set style/cap for tickers that have never been classified, using existing categorize ETF data."""
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE public.ticker t
                    SET style_type = ct.style_type,
                        cap_type   = ct.cap_type,
                        type_from  = 'CAT_ETF'
                    FROM public.categorize_ticker ct
                    WHERE t.symbol        = ct.symbol
                      AND t.exchange      = ct.exchange
                      AND ct.style_type   IS NOT NULL
                      AND t.style_type    IS NULL
                      AND t.invalid       IS NULL;
                """)
    except Error as e:
        raise Exception(f"Error updating style for unclassified tickers: {e}")


def update_style_from_provider_etfs() -> None:
    """Fill style_type for unclassified tickers using provider_etf holdings (value/growth only)."""
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE public.ticker t
                    SET style_type = pe.style_type,
                        type_from  = 'PROVIDER_ETF'
                    FROM public.provider_etf_holding peh
                    JOIN public.provider_etf pe ON pe.id = peh.provider_etf_id
                    WHERE t.id         = peh.ticker_id
                      AND t.style_type IS NULL
                      AND t.invalid    IS NULL
                      AND pe.style_type IN ('value', 'growth');
                """)
    except Error as e:
        raise Exception(f"Error updating ticker style from provider ETFs: {e}")


def fetch_all_with_missing_style() -> list['Ticker']:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Ticker)) as cur:
                cur.execute("""
                    SELECT id, symbol FROM ticker
                    WHERE style_type IS NULL AND invalid IS NULL
                """)
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error fetching tickers with missing style: {e}")


def update_style_from_model_bulk(updates: list[dict]) -> None:
    if not updates:
        return
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    "UPDATE ticker SET style_type = %s, type_from = 'MODEL' WHERE id = %s",
                    [(u["style_type"], u["ticker_id"]) for u in updates]
                )
    except Error as e:
        raise Exception(f"Error bulk-updating ticker style from model: {e}")
