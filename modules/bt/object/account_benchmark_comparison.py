from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Tuple
from psycopg.errors import Error
from modules.core.db import db_pool_instance_bt

@dataclass
class AccountBenchmarkComparison:
    account_id: int
    benchmark_symbol: str
    performance_date: date
    strategy_indexed_value: Decimal
    benchmark_indexed_value: Decimal
    daily_alpha: Decimal

def fetch_previous_comparison_values(account_id: int, symbol: str, prev_date: date) -> Tuple[Decimal, Decimal]:
    try:
        with db_pool_instance_bt.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT strategy_indexed_value, benchmark_indexed_value 
                    FROM account_benchmark_comparison 
                    WHERE account_id = %s AND benchmark_symbol = %s AND performance_date <= %s
                    ORDER BY performance_date DESC LIMIT 1
                """, (account_id, symbol, prev_date))
                res = cur.fetchone()
                return (Decimal(res[0]), Decimal(res[1])) if res else (Decimal('1.0'), Decimal('1.0'))
    except Error as e:
        raise Exception(f"Error fetching previous comparison values: {e}")

def record_benchmark_comparison(entry: AccountBenchmarkComparison):
    try:
        with db_pool_instance_bt.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO account_benchmark_comparison 
                    (account_id, benchmark_symbol, performance_date, strategy_indexed_value, benchmark_indexed_value, daily_alpha)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (account_id, benchmark_symbol, performance_date) DO UPDATE SET
                        strategy_indexed_value = EXCLUDED.strategy_indexed_value,
                        benchmark_indexed_value = EXCLUDED.benchmark_indexed_value,
                        daily_alpha = EXCLUDED.daily_alpha;
                """, (entry.account_id, entry.benchmark_symbol, entry.performance_date, 
                    entry.strategy_indexed_value, entry.benchmark_indexed_value, entry.daily_alpha))
    except Error as e:
        raise Exception(f"Error recording the benchmark comparison: {e}")
