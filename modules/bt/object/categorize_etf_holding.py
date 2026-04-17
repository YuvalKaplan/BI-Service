from typing import Optional, List
from datetime import date, datetime
from psycopg.errors import Error
from dataclasses import dataclass
from modules.core.db import db_pool_instance_bt


@dataclass
class CategorizeEtfHolding:
    categorize_etf_id: int
    holding_date: date
    ticker: str
    id: Optional[int]
    created_at: Optional[datetime]


def insert_holding(categorize_etf_id: int, holding_date: date, tickers: List[str]) -> None:
    if not tickers:
        return

    try:
        with db_pool_instance_bt.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    DELETE FROM categorize_etf_holding
                    WHERE categorize_etf_id = %s
                      AND holding_date = %s;
                """, (categorize_etf_id, holding_date))

                cur.executemany("""
                    INSERT INTO categorize_etf_holding (categorize_etf_id, holding_date, ticker)
                    VALUES (%s, %s, %s);
                """, [(categorize_etf_id, holding_date, symbol) for symbol in tickers])

            conn.commit()

    except Error as e:
        raise Exception(f"Error replacing categorize ETF holdings in BT DB: {e}")
