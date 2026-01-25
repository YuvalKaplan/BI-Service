from datetime import date
from typing import List
from psycopg.errors import Error
from psycopg.rows import class_row, dict_row
import pandas as pd
from dataclasses import dataclass, asdict
from modules.core.db import db_pool_instance

@dataclass
class BestIdea:
    provider_etf_id: str
    symbol: str
    value_date: date
    etf_weight: float | None
    benchmark_weight: float | None
    delta: float | None
    ranking: int | None

def df_to_rows(
    best_ideas: pd.DataFrame,
    provider_etf_id: int,
    value_date: date
) -> list[tuple]:
    rows = []
    for rank, (_, row) in enumerate(best_ideas.iterrows(), start=1):
        rows.append((provider_etf_id, row["symbol"], value_date, float(row["etf_weight"]), float(row["benchmark_weight"]), float(row["delta"]), rank))
    return rows

def insert_bulk(rows: list[tuple]):
    if not rows:
        return

    query = """
        INSERT INTO best_idea
            (provider_etf_id, symbol, value_date, etf_weight, benchmark_weight, delta, ranking)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (provider_etf_id, symbol, value_date)
        DO UPDATE
        SET
            etf_weight = EXCLUDED.etf_weight,
            benchmark_weight = EXCLUDED.benchmark_weight,
            delta = EXCLUDED.delta,
            ranking = EXCLUDED.ranking
        WHERE
            best_idea.etf_weight IS DISTINCT FROM EXCLUDED.etf_weight
            OR best_idea.benchmark_weight IS DISTINCT FROM EXCLUDED.benchmark_weight
            OR best_idea.delta IS DISTINCT FROM EXCLUDED.delta
            OR best_idea.ranking IS DISTINCT FROM EXCLUDED.ranking;
    """

    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(query, rows)
    except Error as e:
        raise Exception(f"Error inserting Best Ideas in bulk: {e}")


@dataclass
class BestIdeaRanked:
    symbol: str
    name: str
    style_type: str
    cap_type: str
    ranking: int
    appearances: int
    max_delta: float
    source_etf_id: int          # The single ID associated with max delta
    all_provider_ids: List[int] # The array of all provider IDs

def fetch_best_ideas_by_ranking(ranking_level: int) -> List[BestIdeaRanked]:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(BestIdeaRanked)) as cur:
                cur.execute('SELECT * FROM ticker WHERE symbol = %s;', (ranking_level,))
                items = cur.fetchall()
        return items
    except Error as e:
        raise Exception(f"Error fetching the BestIdeaResult from the DB: {e}")
                
