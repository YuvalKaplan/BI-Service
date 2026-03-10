from datetime import datetime
from typing import Optional
from psycopg.errors import Error
from psycopg.rows import class_row
from dataclasses import dataclass
from modules.core.db import db_pool_instance_bt

@dataclass
class Account:
    created_at: datetime 
    name: str
    base_currency: str
    strategy_fund_id: int
    id: Optional[int] = None

def fetch_all():
    try:
        with db_pool_instance_bt.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Account)) as cur:
                query_str = "SELECT * FROM account;"
                cur.execute(query_str)
                items = cur.fetchall()
        return items
    except Error as e:
        raise Exception(f"Error fetching the Account list from the DB: {e}")

def fetch_fund(account_id: int):
    try:
        with db_pool_instance_bt.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Account)) as cur:
                cur.execute("""
                    SELECT * FROM account 
                    WHERE id = %s 
                """, (account_id,))
                items = cur.fetchone()
        return items
    except Error as e:
        raise Exception(f"Error fetching the Account details from the DB: {e}")
