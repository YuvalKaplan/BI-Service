from datetime import datetime
from typing import List
from dataclasses import dataclass
from psycopg.errors import Error
from psycopg.rows import class_row
from modules.core.db import db_pool_instance


@dataclass(kw_only=True)
class UniverseEtf:
    """
    An index fund the market is measured by (modules/ticker/index_funds.py): VTI for the US,
    VEA + VWO for International. Its weekly holdings are stored in universe_etf_holding.
    """
    id: int
    created_at: datetime | None = None
    updated_at: datetime | None = None
    disabled: bool = False
    symbol: str
    name: str | None = None
    market: str                           # 'US' | 'International'
    last_downloaded: datetime | None = None


def fetch_enabled() -> List[UniverseEtf]:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(UniverseEtf)) as cur:
                cur.execute('SELECT * FROM universe_etf WHERE NOT disabled ORDER BY id;')
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error fetching the index funds (universe_etf): {e}")


def set_last_downloaded(id: int, when: datetime) -> None:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE universe_etf SET last_downloaded = %s, updated_at = (now() AT TIME ZONE 'utc') WHERE id = %s;",
                            (when, id))
            conn.commit()
    except Error as e:
        raise Exception(f"Error saving the last download of index fund {id}: {e}")
