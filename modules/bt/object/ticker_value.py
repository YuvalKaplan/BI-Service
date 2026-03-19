from datetime import date
from typing import List
from psycopg.errors import Error
from psycopg.rows import class_row, dict_row
import pandas as pd
from dataclasses import dataclass, asdict
from modules.core.db import db_pool_instance_bt

@dataclass
class TickerValue:
    symbol: str
    value_date: date | None
    stock_price: float | None
    market_cap: float | None

def ticker_values_to_df(values: list[TickerValue]) -> pd.DataFrame:
    df = pd.DataFrame(asdict(v) for v in values)

    return (
        df
        .dropna(subset=["symbol", "stock_price", "market_cap"])
        .query("stock_price > 0 and market_cap > 0")
    )


def fetch_price_dates_available_past_period(end_date: date, look_back_days: int) -> list[date]:
    try:
        with db_pool_instance_bt.get_connection() as conn:
            with conn.cursor() as cur:
                query = """
                    SELECT DISTINCT (value_date::date)
                        FROM ticker_value
                        WHERE value_date > %s - (%s * INTERVAL '1 day') 
                        AND value_date <= %s
                        ORDER BY 1;
                """
                cur.execute(query, (end_date, look_back_days, end_date,))
                return [row[0] for row in cur.fetchall()]
    except Error as e:
        raise Exception(f"Error retrieving the list of dates available in the past week: {e}")

def fetch_tickers_availability_dates(symbol: str) -> List[dict]:
    try:
        with db_pool_instance_bt.get_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                query = """
                    SELECT value_date
                    FROM ticker_value
                    WHERE symbol = %s;
                """
                cur.execute(query, (symbol,))
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error retrieving dates availability for a ticker symbol TickerValue data: {e}")
    
def fetch_tickers_by_symbols_on_date(symbols: List[str], value_date: date) -> List[TickerValue]:
    try:
        with db_pool_instance_bt.get_connection() as conn:
            with conn.cursor(row_factory=class_row(TickerValue)) as cur:
                query = """
                    SELECT symbol, value_date, stock_price, market_cap
                    FROM ticker_value
                    WHERE symbol = ANY(%s)
                      AND value_date = %s;
                """
                cur.execute(query, (symbols, value_date))
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error retrieving latest TickerValue data: {e}")
    
def upsert(item: TickerValue):
    try:
        with db_pool_instance_bt.get_connection() as conn:
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

def upsert_bulk(items: List[TickerValue]):
    if not items:
        return

    try:
        with db_pool_instance_bt.get_connection() as conn:
            with conn.cursor() as cur:

                insert_sql = """
                    INSERT INTO ticker_value (
                        symbol,
                        value_date,
                        stock_price,
                        market_cap
                    )
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (symbol, value_date)
                    DO UPDATE SET
                        stock_price = EXCLUDED.stock_price,
                        market_cap = EXCLUDED.market_cap;
                """

                insert_values = [
                    (i.symbol, i.value_date, i.stock_price, i.market_cap)
                    for i in items
                ]

                cur.executemany(insert_sql, insert_values)

            conn.commit()

    except Error as e:
        raise Exception(f"Error inserting ticker values in DB: {e}")