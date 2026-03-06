from datetime import date
from typing import List
from psycopg.errors import Error
from psycopg.rows import class_row
from dataclasses import dataclass
from modules.core.db import db_pool_instance_bt

@dataclass
class FundHolding:
    fund_id: int
    symbol: str
    holding_date: date
    ranking: int 

def fetch_funds_holdings(fund_id: int, eval_date: date):
    try:
        with db_pool_instance_bt.get_connection() as conn:
            with conn.cursor(row_factory=class_row(FundHolding)) as cur:
                cur.execute("""
                    SELECT * FROM fund_holding 
                    WHERE fund_id = %s AND holding_date = %s
                """, (fund_id, eval_date))
                items = cur.fetchall()
        return items
    except Error as e:
        raise Exception(f"Error fetching the Fund Holdings from the DB: {e}")
