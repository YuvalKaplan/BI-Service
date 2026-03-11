from typing import Optional, List
from datetime import date, datetime
from psycopg.errors import Error
from psycopg.rows import class_row
from dataclasses import dataclass
from modules.core.db import db_pool_instance
import pandas as pd

@dataclass
class CategorizeEtfHolding:
    categorize_etf_id: int
    holding_date: date
    ticker: str
    id: Optional[int]
    created_at: Optional[datetime]


def insert_holding(categorize_etf_id, holding_date: date, tickers: List[str]):
    if not tickers:
        return
    
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:

                delete_sql = """
                    DELETE FROM categorize_etf_holding
                    WHERE categorize_etf_id = %s
                      AND holding_date = %s;
                """
                cur.execute(delete_sql, (categorize_etf_id, holding_date))

                insert_sql = """
                    INSERT INTO categorize_etf_holding (
                        categorize_etf_id,
                        holding_date,
                        ticker
                    )
                    VALUES (%s, %s, %s);
                """

                insert_values = [
                    (categorize_etf_id, holding_date, symbol)
                    for symbol in tickers
                ]

                cur.executemany(insert_sql, insert_values)

            conn.commit()

    except Error as e:
        raise Exception(f"Error replacing categorize fund holdings in DB: {e}")