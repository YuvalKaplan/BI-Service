from datetime import datetime
from psycopg.errors import Error
from psycopg.rows import class_row
from dataclasses import dataclass
from modules.core.db import db_pool_instance

@dataclass
class Ticker:
    symbol: str
    created_at: datetime | None
    style: str | None
    cap: str | None
    source: str | None
    isin: str | None
    cik: str | None
    exchange: str | None
    name: str | None
    industry: str | None
    sector: str | None

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
                    INSERT INTO ticker (symbol, cap, style, source)
                    SELECT unnest(%s::text[]), %s, %s, 'cat_etf'
                    ON CONFLICT (symbol)
                    DO UPDATE
                    SET
                        cap = EXCLUDED.cap,
                        style = EXCLUDED.style,
                        source = 'cat_etf'
                    WHERE ticker.cap IS DISTINCT FROM EXCLUDED.cap
                       OR ticker.style IS DISTINCT FROM EXCLUDED.style;
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
