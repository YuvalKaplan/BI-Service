from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Optional
from psycopg.errors import Error
from psycopg.rows import class_row
from modules.core.db import db_pool_instance_bt

@dataclass
class InterestRateConfig:
    effective_date: date
    annual_rate: Decimal
    id: Optional[int] = None

def get_latest_interest_rate(eval_date: date) -> Optional[InterestRateConfig]:
    try:
        with db_pool_instance_bt.get_connection() as conn:
            with conn.cursor(row_factory=class_row(InterestRateConfig)) as cur:
                cur.execute("""
                    SELECT * FROM interest_rate_config 
                    WHERE effective_date <= %s 
                    ORDER BY effective_date DESC LIMIT 1
                """, (eval_date,))
                return cur.fetchone()
    except Error as e:
        raise Exception(f"Error getting latest interest rate: {e}")
