from datetime import date, datetime
from typing import List
from dataclasses import dataclass
from psycopg.errors import Error
from psycopg.rows import class_row
from modules.core.db import db_pool_instance

REGION = 'region'   # parts: 'US' | 'International' - the regions' full-universe (blend) benchmarks
STYLE = 'style'     # parts: 'growth' | 'value' - the US blend's value / growth split


@dataclass(kw_only=True)
class BenchmarkSplit:
    """
    One part of the market split (modules/cron/market_split.py): a benchmark's float-adjusted
    total (whole company cap x float factor, summed over its holdings on as_of_date) and its share
    (weight) of its split's total. Funds with "split": "market" read the latest split on or before
    their rebalancing date.
    """
    id: int | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    as_of_date: date          # the benchmark snapshot date
    split_type: str           # REGION | STYLE
    part: str
    benchmark_id: int
    companies: int
    float_cap: float          # USD
    weight: float             # fraction of the split's total


def replace_for_date(as_of_date: date, split_type: str, items: List[BenchmarkSplit]) -> None:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('DELETE FROM benchmark_split WHERE as_of_date = %s AND split_type = %s;', (as_of_date, split_type))
                cur.executemany(
                    'INSERT INTO benchmark_split (as_of_date, split_type, part, benchmark_id, companies, float_cap, weight) '
                    'VALUES (%s, %s, %s, %s, %s, %s, %s);',
                    [(i.as_of_date, i.split_type, i.part, i.benchmark_id, i.companies, i.float_cap, i.weight) for i in items],
                )
            conn.commit()
    except Error as e:
        raise Exception(f"Error saving the {split_type} market split for {as_of_date}: {e}")


def fetch_in_force(split_type: str, as_of: date) -> List[BenchmarkSplit]:
    """The split's parts from its latest date on or before as_of - else its earliest (a date before
    splits were kept: the sim, the replay) - else empty."""
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(BenchmarkSplit)) as cur:
                cur.execute(
                    """
                    SELECT * FROM benchmark_split
                    WHERE split_type = %s AND as_of_date = COALESCE(
                        (SELECT max(as_of_date) FROM benchmark_split WHERE split_type = %s AND as_of_date <= %s),
                        (SELECT min(as_of_date) FROM benchmark_split WHERE split_type = %s))
                    ORDER BY part;
                    """,
                    (split_type, split_type, as_of, split_type),
                )
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error fetching the {split_type} market split in force on {as_of}: {e}")


def weights_in_force(split_type: str, as_of: date) -> tuple[date, dict[str, float]]:
    """(as_of_date, {part: weight}) of the split in force on as_of. Raises when none is stored."""
    rows = fetch_in_force(split_type, as_of)
    if not rows:
        raise Exception(f"No {split_type} market split stored: run scripts/current_market_split.py")
    return rows[0].as_of_date, {r.part: r.weight for r in rows}
