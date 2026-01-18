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
    invalid: str | None

    def __init__(self, symbol: str, created_at: datetime | None = None, 
                 source: str | None = None, style_type: str | None = None, cap_type: str | None = None, type_from: str | None = None, 
                 isin: str | None = None, cik: str | None = None, exchange: str | None = None, 
                 name: str | None = None, industry: str | None = None, sector: str | None = None, invalid: str | None = None):
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
        self.invalid = invalid

def sync_tickers_with_etf_holdings():
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("CALL public.sync_tickers_symbols();")
        
    except Error as e:
        raise Exception(f"Error executing stored procedure: {e}")

def fetch_by_symbol(symbol: str):
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Ticker)) as cur:
                cur.execute('SELECT * FROM ticker WHERE symbol = %s;', (symbol,))
                item = cur.fetchone()
        return item
    except Error as e:
        raise Exception(f"Error fetching the Ticker from the DB: {e}")

def fetch_by_symbols(symbols: list[str]):
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Ticker)) as cur:
                query_str = """
                    SELECT * FROM ticker
                    WHERE id = ANY(%s);
                """
                cur.execute(query_str, (symbols,))
                items = cur.fetchall()
        return items
    except Error as e:
        raise Exception(f"Error fetching the Ticker list page from the DB: {e}")

def upsert_tickers_style_and_cap(symbols: list[str], cap_type: str, style_type: str):
    if not symbols:
        return

    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                query = """
                    INSERT INTO ticker (symbol, cap_type, style_type, source, type_from)
                    SELECT unnest(%s::text[]), %s, %s, 'cat_etf', 'cat_etf'
                    ON CONFLICT (symbol)
                    DO UPDATE
                    SET
                        cap_type = EXCLUDED.cap_type,
                        style_type = EXCLUDED.style_type,
                        type_from = 'cat_etf'
                    WHERE ticker.cap_type IS DISTINCT FROM EXCLUDED.cap_type
                       OR ticker.style_type IS DISTINCT FROM EXCLUDED.style_type;
                """

                cur.execute(query, (symbols, cap_type, style_type))

    except Error as e:
        raise Exception(f"Error upserting Tickers: {e}")

def update_info(item: Ticker):
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                insert_query = "UPDATE ticker SET isin =%s, cik = %s, name = %s, industry = %s, sector = %s WHERE symbol = %s;"
                cur.execute(insert_query, (item.isin, item.cik, item.name, item.industry, item.sector, item.symbol))

    except Error as e:
        raise Exception(f"Error updating the Ticker item into the DB: {e}")

def update_invalid(symbol: str, reason: str):
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                insert_query = "UPDATE ticker SET invalid =%s WHERE symbol = %s;"
                cur.execute(insert_query, (reason, symbol))

    except Error as e:
        raise Exception(f"Error updating the Ticker invalid reason into the DB: {e}")
