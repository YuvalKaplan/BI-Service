from datetime import datetime, timezone
from psycopg.errors import Error
from psycopg.rows import class_row
from dataclasses import dataclass
from modules.core.db import db_pool_instance

@dataclass
class BatchRun:
    id: int | None
    created_at: datetime | None
    process: str | None
    activation: str | None
    completed_at: datetime | None

    def __init__(self, process: str, activation: str, id: int | None = None, created_at: datetime | None = None, completed_at: datetime | None = None):
        self.id = id 
        self.created_at = created_at
        self.process = process
        self.activation = activation
        self.completed_at = completed_at

def fetch_by_type(process: str, activation: str):
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(BatchRun)) as cur:
                cur.execute('SELECT * FROM batch_run WHERE process = %s AND activation = %s;', (process, activation))
                items = cur.fetchall()
        return items
    except Error as e:
        raise Exception(f"Error fetching the Batches for type {process} {activation} from the DB: {e}")

def insert(item: BatchRun) -> int:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                insert_query = "INSERT INTO batch_run (process, activation) VALUES (%s, %s) RETURNING id;"
                cur.execute(insert_query, (item.process, item.activation))
                row = cur.fetchone()
                new_id = row[0] if row is not None else None
                if new_id is not None:
                    return int(new_id)

        raise Exception(f"Failed to create new batch entry")
    
    except Error as e:
        raise Exception(f"Error inserting the Batch item into the DB: {e}")
    
def update_completed_at(id: int):
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                insert_query = "UPDATE batch_run SET completed_at = %s WHERE id = %s;"
                cur.execute(insert_query, (datetime.now(timezone.utc), id))
                return

        raise Exception(f"Failed to update Batch completed at")
    
    except Error as e:
        raise Exception(f"Error updating the Batch item into the DB: {e}")

