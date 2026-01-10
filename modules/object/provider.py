from datetime import datetime
from psycopg.errors import Error
from psycopg.rows import class_row, dict_row
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

class DatePosition(BaseModel):
    row: int
    col: int
    max_row_scan: Optional[int] = 0

class DateFormat(BaseModel):
    none: bool = False
    in_file_name: bool = False
    format: str
    single: Optional[DatePosition] = None
    
class Mapping(BaseModel):
    sheet: Optional[str] = None
    product_column: Optional[str] = None
    product_symbol: Optional[str] = None
    skip_rows: int = 0
    header_row: int = 0
    header_data_gap: int = 0
    multi_row_header: Optional[int] = 1
    no_prefix_headers: list[str] = []
    columns: Dict[str, Optional[str]]
    date: DateFormat
    remove_tickers: list[str] = []
    

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

def fetch_active_providers():
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Provider)) as cur:
                query_str = "SELECT * FROM provider WHERE NOT disabled ORDER BY name;"
                cur.execute(query_str)
                items = cur.fetchall()
        return items
    except Error as e:
        raise Exception(f"Error fetching the Provider list page from the DB: {e}")


def get_collection_stats(ids: list[int], start: datetime) -> list[dict]:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                query_str = """
                    SELECT p.id, p.name, COUNT(DISTINCT ped.id) AS downloaded, COUNT(DISTINCT pe.id) AS available 
                    FROM provider as p
                    LEFT OUTER JOIN provider_etf as ped ON p.id = ped.provider_id AND NOT ped.disabled AND ped.last_downloaded >= %s
                    LEFT OUTER JOIN provider_etf as pe ON p.id = pe.provider_id AND NOT pe.disabled
                    WHERE p.id = ANY(%s)
                    GROUP BY p.id, p.name
                    HAVING COUNT(DISTINCT pe.id) > 0
                    ORDER BY p.name
                """
                cur.execute(query_str, (start, ids))
                items = cur.fetchall()
        return items
    except Error as e:
        raise Exception(f"Error fetching the Provider list stats from the DB: {e}")
