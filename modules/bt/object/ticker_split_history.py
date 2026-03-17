from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Optional, List
from psycopg.errors import Error
from psycopg.rows import dict_row
from modules.core.db import db_pool_instance_bt

@dataclass
class TickerSplitHistory:
    symbol: str
    date: date
    numerator: Decimal
    denominator: Decimal
    id: Optional[int] = None
    
def insert_split_bulk(items: List[TickerSplitHistory]):
    if not items:
        return
    
    symbol = items[0].symbol

    # Use a dictionary to keep only the first occurrence of each date
    seen_dates = {}
    unique_items: List[TickerSplitHistory] = []
    
    for item in items:
        if item.date not in seen_dates:
            seen_dates[item.date] = True
            unique_items.append(item)

    try:
        with db_pool_instance_bt.get_connection() as conn:
            with conn.cursor() as cur:

                delete_sql = """
                    DELETE FROM ticker_split_history
                    WHERE symbol = %s;
                """
                cur.execute(delete_sql, (symbol,))

                insert_sql = """
                    INSERT INTO ticker_split_history (
                        symbol,
                        date,
                        numerator,
                        denominator
                    )
                    VALUES (%s, %s, %s, %s);
                """

                insert_values = [
                    (i.symbol, i.date, i.numerator, i.denominator)
                    for i in unique_items
                ]

                cur.executemany(insert_sql, insert_values)

            conn.commit()

    except Error as e:
        raise Exception(f"Error replacing splits for ticker symbol: {e}")
    