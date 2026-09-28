from datetime import date
from typing import List
from dataclasses import dataclass
from psycopg import sql
from psycopg.errors import Error
from psycopg.rows import class_row
from modules.core.db import db_pool_instance


@dataclass
class UniverseCompany:
    """
    One company of the large-cap universe on a screen date: its master ticker when the universe
    was built (read through the current master), region ('US' | 'International') and company
    market cap in USD. Written by modules/cron/universe_builder.py; the benchmarks are formed
    from it (modules/cron/benchmark_generator.py, and the sim backfill).
    """
    screen_date: date
    ticker_id: int
    region: str
    market_cap: float


_COLUMNS = ('screen_date', 'ticker_id', 'region', 'market_cap')
_INSERT_SQL = sql.SQL("INSERT INTO universe_company ({columns}) VALUES ({placeholders});").format(
    columns=sql.SQL(", ").join(map(sql.Identifier, _COLUMNS)),
    placeholders=sql.SQL(", ").join(sql.Placeholder() for _ in _COLUMNS),
)


def replace_for_date(screen_date: date, items: List[UniverseCompany]) -> None:
    """Replaces the universe stored for screen_date with `items`."""
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('DELETE FROM universe_company WHERE screen_date = %s;', (screen_date,))
                if items:
                    cur.executemany(_INSERT_SQL, [tuple(getattr(i, c) for c in _COLUMNS) for i in items])
            conn.commit()
    except Error as e:
        raise Exception(f"Error replacing the universe for {screen_date}: {e}")


def fetch_for_date(screen_date: date) -> List[UniverseCompany]:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(UniverseCompany)) as cur:
                cur.execute(
                    f"SELECT {', '.join(_COLUMNS)} FROM universe_company WHERE screen_date = %s ORDER BY market_cap DESC;",
                    (screen_date,),
                )
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error fetching the universe for {screen_date}: {e}")


def fetch_latest_date(up_to: date | None = None) -> date | None:
    """The latest universe date on or before up_to (default: any)."""
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                if up_to is None:
                    cur.execute('SELECT MAX(screen_date) FROM universe_company;')
                else:
                    cur.execute('SELECT MAX(screen_date) FROM universe_company WHERE screen_date <= %s;', (up_to,))
                row = cur.fetchone()
                return row[0] if row else None
    except Error as e:
        raise Exception(f"Error fetching the latest universe date: {e}")
