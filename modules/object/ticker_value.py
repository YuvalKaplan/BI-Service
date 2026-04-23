from datetime import date
from typing import List
from psycopg.errors import Error
from psycopg.rows import class_row, dict_row
import pandas as pd
from dataclasses import dataclass, asdict
from modules.core.db import db_pool_instance

@dataclass
class TickerValue:
    ticker_id: int
    value_date: date | None
    stock_price: float | None
    market_cap: float | None

def ticker_values_to_df(values: list[TickerValue]) -> pd.DataFrame:
    df = pd.DataFrame(asdict(v) for v in values)

    return (
        df
        .dropna(subset=["ticker_id", "stock_price", "market_cap"])
        .query("stock_price > 0 and market_cap > 0")
    )


def fetch_latest_market_caps_within_window(ticker_ids: List[int], as_of_date: date, days: int) -> List[TickerValue]:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(TickerValue)) as cur:
                query = """
                    SELECT DISTINCT ON (ticker_id) ticker_id, value_date, stock_price, market_cap
                    FROM ticker_value
                    WHERE ticker_id = ANY(%s)
                      AND value_date >= %s - (%s * INTERVAL '1 day')
                      AND value_date <= %s + (%s * INTERVAL '1 day')
                      AND market_cap IS NOT NULL
                    ORDER BY ticker_id, value_date DESC;
                """
                cur.execute(query, (ticker_ids, as_of_date, days, as_of_date, days))
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error fetching latest market caps within window: {e}")


def upsert(item: TickerValue) -> None:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                query = """
                    INSERT INTO ticker_value (ticker_id, value_date, stock_price, market_cap)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (ticker_id, value_date)
                    DO UPDATE
                    SET
                        stock_price = EXCLUDED.stock_price,
                        market_cap  = EXCLUDED.market_cap
                    WHERE ticker_value.stock_price IS DISTINCT FROM EXCLUDED.stock_price
                    OR ticker_value.market_cap  IS DISTINCT FROM EXCLUDED.market_cap;
                """
                cur.execute(query, (item.ticker_id, item.value_date, item.stock_price, item.market_cap))

    except Error as e:
        raise Exception(f"Error inserting the Batch item into the DB: {e}")

def upsert_bulk(items: List[TickerValue]) -> None:
    if not items:
        return

    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:

                insert_sql = """
                    INSERT INTO ticker_value (
                        ticker_id,
                        value_date,
                        stock_price,
                        market_cap
                    )
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (ticker_id, value_date)
                    DO UPDATE SET
                        stock_price = EXCLUDED.stock_price,
                        market_cap = EXCLUDED.market_cap;
                """

                insert_values = [
                    (i.ticker_id, i.value_date, i.stock_price, i.market_cap)
                    for i in items
                ]

                cur.executemany(insert_sql, insert_values)

            conn.commit()

    except Error as e:
        raise Exception(f"Error inserting ticker values in DB: {e}")