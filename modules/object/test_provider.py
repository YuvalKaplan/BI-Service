from datetime import datetime
from dataclasses import dataclass
from psycopg.errors import Error
from modules.core.db import db_pool_instance

AWAITING_APPROVAL = 'Awaiting approval by admin'


@dataclass(kw_only=True)
class TestProvider:
    """
    A fund company whose actively managed equity ETFs were found from the SEC's N-CEN filings
    and FMP (modules/sec/etf_profile.py) - parallel to provider, without its scraping
    configuration. Disabled until an admin approves it.
    """
    id: int | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    disabled: bool = True
    disabled_reason: str | None = AWAITING_APPROVAL
    name: str


def ensure_by_name(name: str) -> int:
    """The id of the provider with this name, inserted (disabled, awaiting approval) if new."""
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO test_provider (name) VALUES (%s) ON CONFLICT (name) DO NOTHING RETURNING id;", (name,)
                )
                row = cur.fetchone()
                if row is None:
                    cur.execute("SELECT id FROM test_provider WHERE name = %s;", (name,))
                    row = cur.fetchone()
            conn.commit()
        if row is None:
            raise Exception(f"No test provider row for '{name}'")
        return int(row[0])
    except Error as e:
        raise Exception(f"Error saving test provider '{name}': {e}")
