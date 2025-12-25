from psycopg.errors import Error
from dataclasses import dataclass
from datetime import datetime, timezone
from modules.core.db import db_pool_instance

@dataclass
class Log:
    id: int
    created_at: datetime
    process: str
    log_type: str
    code: str | None
    msg: str

    def __init__(self, type, code, msg):
        self.created_at = datetime.now(timezone.utc)
        self.process = 'Service'
        self.log_type = type
        self.code = code
        self.msg = msg

def insert(item: Log):
    try:
        print(item.msg)
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                insert_query = "INSERT INTO log (process, log_type, code, msg) VALUES (%s, %s, %s, %s) RETURNING id;"
                cur.execute(insert_query, (item.process, item.log_type, item.code, item.msg))
                row = cur.fetchone()
                new_id = row[0] if row is not None else None
                conn.commit()
                return new_id
    except Error as e:
        print(f"Error inserting the log record into the DB: {e}")
        return None


