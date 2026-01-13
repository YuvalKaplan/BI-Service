from datetime import date
from typing import List
from psycopg.errors import Error
from psycopg.rows import class_row, dict_row

from dataclasses import dataclass
from modules.core.db import db_pool_instance

@dataclass
class TickerValue:
    symbol: str
    value_date: date | None
    stock_price: float | None
    market_cap: float | None

def upsert(item: TickerValue):
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                query = """
                    INSERT INTO ticker_value (symbol, value_date, stock_price, market_cap)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (symbol, value_date)
                    DO UPDATE
                    SET
                        stock_price = EXCLUDED.stock_price,
                        market_cap  = EXCLUDED.market_cap
                    WHERE ticker_value.stock_price IS DISTINCT FROM EXCLUDED.stock_price
                    OR ticker_value.market_cap  IS DISTINCT FROM EXCLUDED.market_cap;
                """
                cur.execute(query, (item.symbol, item.value_date, item.stock_price, item.market_cap))
    
    except Error as e:
        raise Exception(f"Error inserting the Batch item into the DB: {e}")

def fetch_tickers_by_symbols(symbols: List[str]) -> List[TickerValue]:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(TickerValue)) as cur:
                query = "SELECT * FROM ticker_value WHERE symbol = ANY(%s);"              
                cur.execute(query, (symbols,))
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error retrieving TickerValue data: {e}")
    
