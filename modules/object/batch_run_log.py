from datetime import datetime
from psycopg.errors import Error
from dataclasses import dataclass
from modules.core.db import db_pool_instance

@dataclass
class BatchRunLog:
    id: int | None
    created_at: datetime | None
    batch_run_id: int
    note: str

    def __init__(self, batch_run_id: int, note: str, id: int | None = None, created_at: datetime | None = None):
        self.id = id 
        self.created_at = created_at
        self.batch_run_id = batch_run_id
        self.note = note

@dataclass
class ActiveUser:
    id: int
    last_run: datetime

def insert(item: BatchRunLog) -> int:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                insert_query = "INSERT INTO batch_run_log (batch_run_id, note) VALUES (%s, %s) RETURNING id;"
                cur.execute(insert_query, (item.batch_run_id, item.note))
                row = cur.fetchone()
                new_id = row[0] if row is not None else None
                if new_id is not None:
                    return int(new_id)

        raise Exception(f"Failed to create new Batch run log entry")
    
    except Error as e:
        raise Exception(f"Error inserting the Batch run log item into the DB: {e}")
    
