from datetime import datetime
from psycopg.errors import Error
from psycopg.rows import class_row, dict_row
from dataclasses import dataclass
from pydantic import BaseModel
from typing import Dict, Optional
from modules.core.db import db_pool_instance_bt
from modules.object.provider import Mapping

@dataclass
class Provider:
    id: int
    created_at: datetime | None
    disabled: bool | None
    disabled_reason: str | None
    name: str | None
    domain: str | None
    file_format: str | None

def getMappingFromJson(data: dict) -> Mapping:
    return Mapping.model_validate(data)

def fetch_by_id(id: int) -> Provider | None:
    try:
        with db_pool_instance_bt.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Provider)) as cur:
                cur.execute('SELECT * FROM provider WHERE id = %s;', (id,))
                item = cur.fetchone()
        return item
    except Error as e:
        raise Exception(f"Error fetching the Provider from the DB: {e}")

def fetch_by_ids(ids: list[int]) -> list[Provider]:
    try:
        with db_pool_instance_bt.get_connection() as conn:
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
    
def fetch_by_etf_id(etf_id: int) -> Provider:
    try:
        with db_pool_instance_bt.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Provider)) as cur:
                query_str = """
                    SELECT DISTINCT p.* FROM provider AS p
                    INNER JOIN provider_etf AS pe ON p.id = pe.provider_id 
                    WHERE pe.id = %s
                    ;
                """
                cur.execute(query_str, (etf_id,))
                item = cur.fetchone()
            if item is None:
                raise Exception(f"Provider not associated to provider ETF ID {etf_id}")
        return item
    except Error as e:
        raise Exception(f"Error fetching the Provider for associated provider ETF ID from the DB: {e}")

def update_domain(item: Provider) -> None:
    try:
        with db_pool_instance_bt.get_connection() as conn:
            with conn.cursor() as cur:
                insert_query = "UPDATE provider SET domain = %s WHERE id = %s;"
                cur.execute(insert_query, (item.domain, item.id))
                return

        raise Exception(f"Failed to update provider domain")
    except Error as e:
        raise Exception(f"Error updating the Provider item into the DB: {e}")

def fetch_active_providers() -> list[Provider]:
    try:
        with db_pool_instance_bt.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Provider)) as cur:
                query_str = "SELECT * FROM provider WHERE NOT disabled ORDER BY name;"
                cur.execute(query_str)
                items = cur.fetchall()
        return items
    except Error as e:
        raise Exception(f"Error fetching the Provider list page from the DB: {e}")

