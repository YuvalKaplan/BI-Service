import json
import log
from datetime import datetime
from psycopg.errors import Error
from psycopg.rows import class_row
from dataclasses import dataclass
from modules.core.db import db_pool_instance

@dataclass
class Ticker:
    symbol: str
    created_at: datetime | None
    source: str | None
    style_type: str | None
    cap_type: str | None
    type_from: str | None
    isin: str | None
    cik: str | None
    exchange: str | None
    name: str | None
    industry: str | None
    sector: str | None
    currency: str | None
    esg_factors: dict | None
    esg_qualified: bool | None
    invalid: str | None

    def __init__(self, symbol: str, created_at: datetime | None = None,
                 source: str | None = None, style_type: str | None = None, cap_type: str | None = None, type_from: str | None = None,
                 isin: str | None = None, cik: str | None = None, exchange: str | None = None,
                 name: str | None = None, industry: str | None = None, sector: str | None = None,
                 currency: str | None = None, esg_factors: dict | None = None,
                 esg_qualified: bool | None = None, invalid: str | None = None):
        self.symbol = symbol
        self.created_at = created_at
        self.source = source
        self.style_type = style_type
        self.cap_type = cap_type
        self.type_from = type_from
        self.isin = isin
        self.cik = cik
        self.exchange = exchange
        self.name = name
        self.industry = industry
        self.sector = sector
        self.currency = currency
        self.esg_factors = esg_factors
        self.esg_qualified = esg_qualified
        self.invalid = invalid


def fetch_by_symbol(symbol: str) -> Ticker | None:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Ticker)) as cur:
                cur.execute('SELECT * FROM ticker WHERE symbol = %s;', (symbol,))
                item = cur.fetchone()
        return item
    except Error as e:
        raise Exception(f"Error fetching the Ticker from the DB: {e}")

def fetch_by_symbols(symbols: list[str]) -> list[Ticker]:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Ticker)) as cur:
                query_str = """
                    SELECT * FROM ticker
                    WHERE symbol = ANY(%s);
                """
                cur.execute(query_str, (symbols,))
                items = cur.fetchall()
        return items
    except Error as e:
        raise Exception(f"Error fetching the Ticker list page from the DB: {e}")

def upsert(item: Ticker) -> None:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                query = """
                    INSERT INTO ticker (symbol, isin, cik, name, exchange, industry, sector, currency, source, type_from)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol)
                    DO UPDATE
                    SET isin = EXCLUDED.isin,
                        cik = EXCLUDED.cik,
                        name = EXCLUDED.name,
                        exchange = EXCLUDED.exchange,
                        industry = EXCLUDED.industry,
                        sector = EXCLUDED.sector,
                        currency = EXCLUDED.currency,
                        source = EXCLUDED.source,
                        type_from = EXCLUDED.type_from;
                """
                cur.execute(query, (item.symbol, item.isin, item.cik, item.name, item.exchange, item.industry, item.sector, item.currency, item.source, item.type_from))
    
    except Error as e:
        raise Exception(f"Error upserting ticker into the DB: {e}")

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

def update_esg_data(symbol: str, esg_qualified: bool, esg_factors: dict) -> None:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE ticker
                    SET esg_qualified = %s,
                        esg_factors   = %s
                    WHERE symbol = %s;
                """, (esg_qualified, json.dumps(esg_factors), symbol))
    except Error as e:
        raise Exception(f"Error updating esg data for {symbol}: {e}")

def update_invalid(symbol: str, reason: str) -> None:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                insert_query = "UPDATE ticker SET invalid =%s WHERE symbol = %s;"
                cur.execute(insert_query, (reason, symbol))

    except Error as e:
        raise Exception(f"Error updating the Ticker invalid reason into the DB: {e}")


def sanitize() -> None:
    """Mark any ticker table rows with non-standard symbols as invalid.

    Delegates entirely to the sanitize_tickers() SQL function.
    """
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT sanitize_tickers();')
    except Error as e:
        raise Exception(f"Error sanitizing tickers: {e}")


def update_style_from_categorization_etfs() -> None:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE public.ticker t
                    SET style_type = ce.style_type,
                        cap_type   = ce.cap_type,
                        type_from  = 'CAT_ETF'
                    FROM public.categorize_etf_holding ceh
                    JOIN public.categorize_etf ce ON ce.id = ceh.categorize_etf_id
                    WHERE t.symbol   = ceh.ticker
                      AND ce.usage   = 'style'
                      AND ce.style_type IS NOT NULL
                      AND t.invalid  IS NULL;
                """)

    except Error as e:
        raise Exception(f"Error updating ticker style from categorization ETFs: {e}")

def update_style_from_provider_etfs() -> None:
    """Fill style_type for unclassified tickers using the style_type set on provider_etf holdings.
    Only 'value' and 'growth' are applied — 'blend' is intentionally excluded."""
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE public.ticker t
                    SET style_type = pe.style_type,
                        type_from  = 'PROVIDER_ETF'
                    FROM public.provider_etf_holding peh
                    JOIN public.provider_etf pe ON pe.id = peh.provider_etf_id
                    WHERE t.symbol     = peh.ticker
                      AND t.style_type IS NULL
                      AND t.invalid    IS NULL
                      AND pe.style_type IN ('value', 'growth');
                """)

    except Error as e:
        raise Exception(f"Error updating ticker style from provider ETFs: {e}")

