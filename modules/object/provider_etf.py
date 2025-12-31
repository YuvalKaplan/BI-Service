from datetime import datetime, timezone
from psycopg.errors import Error
from psycopg.rows import class_row
from dataclasses import dataclass
from modules.core.db import db_pool_instance
from modules.object.provider import Provider 


@dataclass
class ProviderEtf:
    id: int | None
    created_at: datetime | None
    provider_id: int | None
    disabled: bool | None
    disabled_reason: str | None
    name: str | None
    isin: str | None
    cap_type: str | None
    style_type: str | None
    benchmark: str | None
    trading_since: datetime | None
    number_of_managers: int | None
    url: str | None
    wait_pre_events: str | None
    wait_post_events: str | None
    events: dict | None
    trigger_download: dict | None
    mapping: dict | None
    file_format: str | None
    last_downloaded: datetime | None

@dataclass
class EtfDownload:
    provider: Provider
    etf: ProviderEtf
    file_name: str | None
    data: bytes | None

def fetch_by_id(id: int):
    with db_pool_instance.get_connection() as conn:
        with conn.cursor(row_factory=class_row(ProviderEtf)) as cur:
            cur.execute('SELECT * FROM provider_etf WHERE id = %s;', (id,))
            item = cur.fetchone()
    return item

def fetch_by_provider_id(provider_id: int):
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(ProviderEtf)) as cur:
                query_str = """
                    SELECT * FROM provider_etf
                    WHERE provider_id = %s
                    LIMIT 2
                    ;
                """
                cur.execute(query_str, (provider_id,))
                items = cur.fetchall()
        return items
    except Error as e:
        raise Exception(f"Error fetching the Provider ETFs for provider ID from the DB: {e}")


def update_last_download(id: int):
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                insert_query = "UPDATE provider_etf SET last_downloaded = %s WHERE id = %s;"
                cur.execute(insert_query, (datetime.now(timezone.utc), id))
                conn.commit()
                return

        raise Exception(f"Failed to update last download")
    
    except Error as e:
        raise Exception(f"Error updating the Provider item in the DB: {e}")
