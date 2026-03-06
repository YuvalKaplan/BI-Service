from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Optional
from psycopg.rows import class_row
from modules.core.db import db_pool_instance_bt

@dataclass
class BenchmarkValue:
    symbol: str
    value_date: date
    price: Decimal

def fetch_benchmark_price(symbol: str, eval_date: date) -> Optional[BenchmarkValue]:
    """
    Fetches the single price for a specific benchmark on a specific date.
    """
    try:
        with db_pool_instance_bt.get_connection() as conn:
            with conn.cursor(row_factory=class_row(BenchmarkValue)) as cur:
                query = """
                    SELECT symbol, value_date, price
                    FROM benchmark_value
                    WHERE symbol = %s AND value_date = %s;
                """
                cur.execute(query, (symbol, eval_date))
                return cur.fetchone()
    except Exception as e:
        # In a backtest, missing benchmark data is a critical stop
        raise Exception(f"Error retrieving BenchmarkValue for {symbol} on {eval_date}: {e}")

def fetch_latest_benchmark_price_before(symbol: str, eval_date: date) -> Optional[BenchmarkValue]:
    """
    Helper for 'Yesterday' lookups or handling holidays/weekends.
    Finds the most recent price available strictly before the given date.
    """
    try:
        with db_pool_instance_bt.get_connection() as conn:
            with conn.cursor(row_factory=class_row(BenchmarkValue)) as cur:
                query = """
                    SELECT symbol, value_date, price
                    FROM benchmark_value
                    WHERE symbol = %s AND value_date < %s
                    ORDER BY value_date DESC
                    LIMIT 1;
                """
                cur.execute(query, (symbol, eval_date))
                return cur.fetchone()
    except Exception as e:
        raise Exception(f"Error retrieving latest BenchmarkValue for {symbol} before {eval_date}: {e}")