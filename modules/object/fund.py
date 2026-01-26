from datetime import datetime
from psycopg.errors import Error
from psycopg.rows import class_row
from dataclasses import dataclass
from modules.core.db import db_pool_instance

@dataclass
class Fund:
    id: int
    created_at: datetime 
    style_type: str 
    cap_type: str
    name: str
    last_updated: datetime | None

def fetch_all():
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Fund)) as cur:
                query_str = "SELECT * FROM fund;"
                cur.execute(query_str)
                items = cur.fetchall()
        return items
    except Error as e:
        raise Exception(f"Error fetching the Fund list from the DB: {e}")

