from datetime import datetime
from psycopg.errors import Error
from psycopg.rows import class_row
from dataclasses import dataclass
from modules.core.db import db_pool_instance

@dataclass
class Provider:
    id: int
    created_at: datetime | None
    style_type: str | None
    cap_type: str | None
    name: str | None
    last_updated: datetime | None

def fetch_all():
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Provider)) as cur:
                query_str = "SELECT * FROM fund;"
                cur.execute(query_str)
                items = cur.fetchall()
        return items
    except Error as e:
        raise Exception(f"Error fetching the Fund list from the DB: {e}")

