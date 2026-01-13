from datetime import datetime
from typing import List
from psycopg.errors import Error
from psycopg.rows import class_row, dict_row

from dataclasses import dataclass
from modules.core.db import db_pool_instance

@dataclass
class TickerValue:
    symbol: str
    value_date: datetime | None
    stock_price: float | None
    market_cap: float | None

def insert(items: List[TickerValue]):
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                with cur.copy("COPY ticker_value (symbol, value_date, stock_price, market_cap) FROM STDIN") as copy:
                    for item in items:
                        copy.write_row((item.symbol, item.value_date, item.stock_price, item.market_cap))
    except Error as e:
        raise Exception(f"Error inserting the TickerValue item into the DB: {e}")

def fetch_tickers_by_symbols(symbols: List[str]) -> List[TickerValue]:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(TickerValue)) as cur:
                query = "SELECT * FROM ticker_value WHERE symbol = ANY(%s);"              
                cur.execute(query, (symbols,))
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error retrieving TickerValue data: {e}")
    
