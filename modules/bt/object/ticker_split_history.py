from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Optional, List
from psycopg.errors import Error
from psycopg.rows import class_row
from modules.core.db import db_pool_instance_bt

@dataclass
class TickerSplitHistory:
    symbol: str
    date: date
    numerator: Decimal
    denominator: Decimal
    id: Optional[int] = None

def fetch_split_factors_on_date(symbols: list[str], event_date: date) -> dict[str, float]:
    """
    Retrieves split records for the given symbols on a specific date 
    and returns a dictionary of {symbol: ratio}.
    """
    if not symbols:
        return {}

    try:
        with db_pool_instance_bt.get_connection() as conn:
            # Using class_row to map to your TickerSplitHistory dataclass
            with conn.cursor(row_factory=class_row(TickerSplitHistory)) as cur:
                query = """
                    SELECT *
                    FROM public.ticker_split_history
                    WHERE symbol = ANY(%s)
                      AND date = %s
                """
                cur.execute(query, (symbols, event_date))
                splits = cur.fetchall()
                
                # Calculate the multiplier: (Numerator / Denominator)
                # Example: 2 / 1 = 2.0 (Double shares)
                # Example: 1 / 10 = 0.1 (Reverse split)
                return {
                    s.symbol: float(s.numerator / s.denominator) 
                    for s in splits
                }
    except Exception as e:
        print(f"Error fetching split factors on {event_date}: {e}")
        return {}
    
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
    