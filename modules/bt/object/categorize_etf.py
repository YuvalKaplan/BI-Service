from datetime import datetime, timezone
from psycopg.errors import Error
from psycopg.rows import class_row
from dataclasses import dataclass
from modules.core.db import db_pool_instance_bt


@dataclass
class CategorizeEtf:
    id: int | None
    created_at: datetime | None
    name: str | None
    usage: str | None
    cap_type: str | None
    style_type: str | None
    url: str | None
    wait_pre_events: str | None
    wait_post_events: str | None
    events: dict | None
    trigger_download: dict | None
    mapping: dict | None
    file_format: str | None
    last_downloaded: datetime | None

@dataclass
class CategorizeEtfDownload:
    etf: CategorizeEtf
    file_name: str | None = None
    data: bytes | None = None


def fetch_all(usage: str):
    try:
        with db_pool_instance_bt.get_connection() as conn:
            with conn.cursor(row_factory=class_row(CategorizeEtf)) as cur:
                cur.execute("SELECT * FROM categorize_etf WHERE usage = %s ORDER BY created_at;", (usage,))
                items = cur.fetchall()
        return items
    except Error as e:
        raise Exception(f"Error fetching the Categorize ETFs from the BT DB: {e}")


def update_last_download(id: int):
    try:
        with db_pool_instance_bt.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE categorize_etf SET last_downloaded = %s WHERE id = %s;",
                            (datetime.now(timezone.utc), id))

    except Error as e:
        raise Exception(f"Error updating the Categorize ETF item in the BT DB: {e}")
