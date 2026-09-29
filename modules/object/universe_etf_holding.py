from datetime import date, datetime
from typing import List
from dataclasses import dataclass
from psycopg import sql
from psycopg.errors import Error
from psycopg.rows import class_row
from modules.core.db import db_pool_instance


@dataclass(kw_only=True)
class UniverseEtfHolding:
    """
    One line of an index fund's holdings snapshot (universe_etf), as FMP listed it, with the float
    cap its company was given (modules/ticker/index_funds.py) - kept to trace the breakpoints,
    benchmark cutoffs, float factors and SEC fund profiles back to the holdings behind them.
    ticker_id is set when the line matches a ticker we already have; weight is a fraction.
    """
    id: int | None = None
    created_at: datetime | None = None
    universe_etf_id: int
    holding_date: datetime
    ticker_id: int | None = None
    shares: float | None = None
    market_value: float | None = None
    weight: float | None = None
    symbol: str | None = None
    name: str | None = None
    isin: str | None = None
    cusip: str | None = None
    float_cap: float | None = None


_COLUMNS = ('universe_etf_id', 'holding_date', 'ticker_id', 'shares', 'market_value', 'weight',
            'symbol', 'name', 'isin', 'cusip', 'float_cap')
_INSERT_SQL = sql.SQL("INSERT INTO universe_etf_holding ({columns}) VALUES ({placeholders});").format(
    columns=sql.SQL(", ").join(map(sql.Identifier, _COLUMNS)),
    placeholders=sql.SQL(", ").join(sql.Placeholder() for _ in _COLUMNS),
)


def replace_snapshot(universe_etf_id: int, holding_date: datetime, items: List[UniverseEtfHolding]) -> None:
    """Stores the fund's snapshot for holding_date, replacing one already stored for that date."""
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('DELETE FROM universe_etf_holding WHERE universe_etf_id = %s AND holding_date = %s;',
                            (universe_etf_id, holding_date))
                if items:
                    cur.executemany(_INSERT_SQL, [tuple(getattr(i, c) for c in _COLUMNS) for i in items])
            conn.commit()
    except Error as e:
        raise Exception(f"Error saving the holdings of index fund {universe_etf_id} for {holding_date}: {e}")


def fetch_latest(universe_etf_id: int, up_to: date) -> List[UniverseEtfHolding]:
    """The fund's snapshot from its latest holding date on or before up_to ([] if none)."""
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(UniverseEtfHolding)) as cur:
                cur.execute(
                    """SELECT * FROM universe_etf_holding
                       WHERE universe_etf_id = %s AND holding_date = (
                           SELECT max(holding_date) FROM universe_etf_holding
                           WHERE universe_etf_id = %s AND holding_date < %s::date + 1)
                       ORDER BY id;""",
                    (universe_etf_id, universe_etf_id, up_to),
                )
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error fetching the holdings of index fund {universe_etf_id}: {e}")
