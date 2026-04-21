import sys
from psycopg.errors import Error
from dataclasses import dataclass, field
from datetime import datetime, timezone
from modules.core.db import db_pool_instance

@dataclass
class Log:
    log_type: str
    code: str | int | None
    msg: str
    id: int | None = None
    process: str = 'Service'
    created_at: datetime | None = None

def insert(item: Log) -> int | None:
    try:
        print(item.msg)
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                insert_query = "INSERT INTO log (process, log_type, code, msg) VALUES (%s, %s, %s, %s) RETURNING id;"
                cur.execute(insert_query, (item.process, item.log_type, item.code, item.msg))
                row = cur.fetchone()
                new_id = row[0] if row is not None else None
                return new_id
    except Error as e:
        print(f"[LOG DB ERROR] {e} | Original message: {item.msg}", file=sys.stderr)
        return None


