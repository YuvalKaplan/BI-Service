from datetime import datetime
from psycopg.errors import Error
from psycopg.rows import class_row
from dataclasses import dataclass
from modules.core.db import db_pool_instance_bt

@dataclass
class Fund:
    id: int
    created_at: datetime 
    name: str
    strategy: dict
    active: bool


def fetch_all():
    try:
        with db_pool_instance_bt.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Fund)) as cur:
                query_str = "SELECT * FROM fund WHERE active = true;"
                cur.execute(query_str)
                items = cur.fetchall()
        return items
    except Error as e:
        raise Exception(f"Error fetching the Fund list from the DB: {e}")

def fetch_fund(fund_id: int):
    try:
        with db_pool_instance_bt.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Fund)) as cur:
                cur.execute("""
                    SELECT * FROM fund 
                    WHERE id = %s 
                """, (fund_id,))
                items = cur.fetchone()
        return items
    except Error as e:
        raise Exception(f"Error fetching the Fund from the DB: {e}")

def reset_funds():
    try:
        with db_pool_instance_bt.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM fund_holding_change")
                cur.execute("DELETE FROM fund_holding")
    except Error as e:
        raise Exception(f"Error reseting the Fund holdings in the DB: {e}")