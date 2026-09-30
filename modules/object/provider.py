from datetime import datetime
from dataclasses import dataclass
from psycopg.errors import Error
from psycopg.rows import class_row
from modules.core.db import db_pool_instance


@dataclass(kw_only=True)
class Provider:
    """
    A fund company whose actively managed equity ETFs were found from the SEC's N-CEN filings and
    FMP (modules/sec/etf_profile.py) - named by FMP's etfCompany, else the N-CEN adviser or
    registrant. Nothing gates on it: an ETF's own status decides whether it's used.
    """
    id: int | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    name: str


def ensure_by_name(name: str) -> int:
    """The id of the provider with this name, inserted if new."""
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO provider (name) VALUES (%s) ON CONFLICT (name) DO NOTHING RETURNING id;", (name,)
                )
                row = cur.fetchone()
                if row is None:
                    cur.execute("SELECT id FROM provider WHERE name = %s;", (name,))
                    row = cur.fetchone()
            conn.commit()
        if row is None:
            raise Exception(f"No provider row for '{name}'")
        return int(row[0])
    except Error as e:
        raise Exception(f"Error saving provider '{name}': {e}")


def fetch_by_ids(ids: list[int]) -> list[Provider]:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Provider)) as cur:
                cur.execute('SELECT * FROM provider WHERE id = ANY(%s);', (ids,))
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error fetching the providers: {e}")
