from datetime import date, datetime
from typing import List
from psycopg.errors import Error
from psycopg.rows import class_row
from dataclasses import dataclass
from modules.core.db import db_pool_instance


@dataclass
class Benchmark:
    id: int
    created_at: datetime
    name: str
    region: str
    cap_type: str
    style_type: str
    market_cap_min: int
    disabled: bool


@dataclass
class BenchmarkHolding:
    id: int
    benchmark_id: int
    holding_date: date
    ticker_id: int
    market_cap: float
    weight: float


def fetch_all() -> List[Benchmark]:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Benchmark)) as cur:
                cur.execute('SELECT * FROM public.benchmark WHERE disabled = false ORDER BY id')
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error fetching benchmarks: {e}")


def fetch_by_region_and_style(region: str, style_type: str) -> Benchmark | None:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Benchmark)) as cur:
                cur.execute(
                    'SELECT * FROM public.benchmark WHERE region = %s AND style_type = %s AND disabled = false',
                    (region, style_type)
                )
                return cur.fetchone()
    except Error as e:
        raise Exception(f"Error fetching benchmark by region/style: {e}")


def fetch_latest_holdings(benchmark_id: int, look_back_days: int) -> List[BenchmarkHolding]:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(BenchmarkHolding)) as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM public.benchmark_holding
                    WHERE benchmark_id = %s
                      AND holding_date = (
                          SELECT MAX(holding_date)
                          FROM public.benchmark_holding
                          WHERE benchmark_id = %s
                            AND holding_date > CURRENT_DATE - (%s * INTERVAL '1 day')
                      )
                    """,
                    (benchmark_id, benchmark_id, look_back_days)
                )
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error fetching latest holdings for benchmark {benchmark_id}: {e}")


def fetch_latest_holdings_for_date(benchmark_id: int, holding_date: date) -> List[BenchmarkHolding]:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(BenchmarkHolding)) as cur:
                cur.execute(
                    'SELECT * FROM public.benchmark_holding WHERE benchmark_id = %s AND holding_date = %s',
                    (benchmark_id, holding_date)
                )
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error fetching holdings for benchmark {benchmark_id} on {holding_date}: {e}")


def insert_holdings(
    benchmark_id: int,
    holding_date: date,
    rows: List[tuple],  # (ticker_id, market_cap, weight)
) -> None:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    'DELETE FROM public.benchmark_holding WHERE benchmark_id = %s AND holding_date = %s',
                    (benchmark_id, holding_date)
                )
                cur.executemany(
                    """
                    INSERT INTO public.benchmark_holding (benchmark_id, holding_date, ticker_id, market_cap, weight)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    [(benchmark_id, holding_date, ticker_id, market_cap, weight)
                     for ticker_id, market_cap, weight in rows]
                )
    except Error as e:
        raise Exception(f"Error inserting holdings for benchmark {benchmark_id}: {e}")
