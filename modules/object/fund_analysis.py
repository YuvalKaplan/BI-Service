from datetime import date
from typing import List
from dataclasses import dataclass
from psycopg.errors import Error
from psycopg.rows import class_row
from modules.core.db import db_pool_instance


@dataclass
class FundAnalysis:
    """
    One company in one of a fund's constituent ETFs, as the fund saw it on a recalculation
    date: the inputs and result of the active-weight calculation, whether or not the company
    became a best idea. ticker_id is the ticker the calculation used — the master when
    share-class listings were consolidated (master_used).
    """
    fund_id: int
    as_of_date: date
    provider_etf_id: int
    holding_date: date
    ticker_id: int
    benchmark_id: int | None
    benchmark_date: date | None
    market_cap: float | None
    master_used: bool
    etf_weight: float | None
    benchmark_weight: float | None
    delta: float | None
    ranking: int | None
    note: str


_COLUMNS = (
    'fund_id', 'as_of_date', 'provider_etf_id', 'holding_date', 'ticker_id', 'benchmark_id', 'benchmark_date',
    'market_cap', 'master_used', 'etf_weight', 'benchmark_weight', 'delta', 'ranking', 'note',
)


def replace_for_fund_date(fund_id: int, as_of_date: date, items: List[FundAnalysis]) -> None:
    """Replaces all of a fund's rows for as_of_date with `items` (deleting even if empty)."""
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('DELETE FROM fund_analysis WHERE fund_id = %s AND as_of_date = %s;', (fund_id, as_of_date))
                if items:
                    cur.executemany(
                        f"INSERT INTO fund_analysis ({', '.join(_COLUMNS)}) VALUES ({', '.join(['%s'] * len(_COLUMNS))});",
                        [tuple(getattr(i, c) for c in _COLUMNS) for i in items],
                    )
            conn.commit()
    except Error as e:
        raise Exception(f"Error replacing fund analysis for fund {fund_id} on {as_of_date}: {e}")


def fetch_for_fund_date(fund_id: int, as_of_date: date) -> List[FundAnalysis]:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(FundAnalysis)) as cur:
                cur.execute(
                    f"SELECT {', '.join(_COLUMNS)} FROM fund_analysis WHERE fund_id = %s AND as_of_date = %s "
                    "ORDER BY provider_etf_id, ranking NULLS LAST, delta DESC NULLS LAST;",
                    (fund_id, as_of_date),
                )
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error fetching fund analysis for fund {fund_id} on {as_of_date}: {e}")


def delete_all_for_fund(fund_id: int) -> None:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('DELETE FROM fund_analysis WHERE fund_id = %s;', (fund_id,))
            conn.commit()
    except Error as e:
        raise Exception(f"Error deleting fund analysis for fund {fund_id}: {e}")
