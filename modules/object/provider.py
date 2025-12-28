from datetime import datetime
from psycopg.errors import Error
from psycopg.rows import class_row
from dataclasses import dataclass
from pydantic import BaseModel
from typing import Dict, Optional
from modules.core.db import db_pool_instance

@dataclass
class Provider:
    id: int | None
    created_at: datetime | None
    disabled: bool | None
    disabled_reason: str | None
    name: str | None
    domain: str | None
    url_start: str | None
    wait_pre_events: str | None
    wait_post_events: str | None
    events: dict | None
    trigger_download: dict | None
    mapping: dict | None
    file_format: str | None

# 1. Define the nested structures first
class WeightConfig(BaseModel):
    is_percent: bool

class FormatConfig(BaseModel):
    date: str
    weight: WeightConfig

# 2. Define the main model
class Mapping(BaseModel):
    sheet: Optional[str]
    columns: Dict[str, str]
    format: FormatConfig
    header_row: int
    skip_rows: int
    remove_tickers: list[str]

def getMappingFromJson(data: dict) -> Mapping:
    return Mapping.model_validate(data)

def fetch_by_id(id: int):
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Provider)) as cur:
                cur.execute('SELECT * FROM provider WHERE id = %s;', (id,))
                item = cur.fetchone()
        return item
    except Error as e:
        raise Exception(f"Error fetching the Provider from the DB: {e}")

def fetch_by_ids(ids: list[int]):
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Provider)) as cur:
                query_str = """
                    SELECT * FROM provider
                    WHERE id = ANY(%s)
                    ;
                """
                cur.execute(query_str, (ids,))
                items = cur.fetchall()
        return items
    except Error as e:
        raise Exception(f"Error fetching the Provider list page from the DB: {e}")

def update_domain(item: Provider):
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                insert_query = "UPDATE provider SET domain = %s WHERE id = %s;"
                cur.execute(insert_query, (item.domain, item.id))
                conn.commit()
                return

        raise Exception(f"Failed to update provider domain")
    except Error as e:
        raise Exception(f"Error updating the Provider item into the DB: {e}")
