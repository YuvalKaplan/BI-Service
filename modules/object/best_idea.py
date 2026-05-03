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
    ticker_id: int
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
        rows.append((provider_etf_id, int(row["ticker_id"]), value_date, float(row["etf_weight"]), float(row["benchmark_weight"]), float(row["delta"]), rank))
    return rows

def insert_bulk(rows: list[tuple]) -> None:
    if not rows:
        return

    query = """
        INSERT INTO best_idea
            (provider_etf_id, ticker_id, value_date, etf_weight, benchmark_weight, delta, ranking)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (provider_etf_id, ticker_id, value_date)
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
    ticker_id: int
    ranking: int
    appearances: int
    max_delta: float
    source_etf_id: int
    all_provider_ids: List[int]

def fetch_best_ideas_by_ranking(ranking_level: int, style_type: str, cap_type: str, as_of_date: date, provider_etf_ids: list[int], exchanges: list[str] = [], esg_only: bool = False, country_type: str = 'all') -> List[BestIdeaRanked]:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(BestIdeaRanked)) as cur:
                cur.execute('SELECT * FROM get_best_ideas_by_ranking(%s, %s, %s, %s, %s, %s, %s, %s);', (ranking_level, style_type, cap_type, as_of_date, provider_etf_ids, exchanges, esg_only, country_type))
                items = cur.fetchall()
        return items
    except Error as e:
        raise Exception(f"Error fetching the BestIdeaResult from the DB: {e}")

def fetch_all_as_df(as_of_date: date, ranking_level: int) -> pd.DataFrame:
    """
    Load one row per (provider_etf_id, ticker_id) for all best ideas within
    the lookback window, including ticker attributes needed for per-fund filtering.
    Returned DataFrame columns: provider_etf_id, ticker_id, value_date, ranking,
    delta, style_type, exchange, country, esg_qualified, market_cap.
    """
    sql = """
        WITH latest_per_etf_ticker AS (
            SELECT DISTINCT ON (bi.provider_etf_id, bi.ticker_id)
                bi.provider_etf_id,
                bi.ticker_id,
                bi.value_date,
                bi.ranking,
                bi.delta
            FROM best_idea bi
            WHERE bi.value_date BETWEEN %(date)s - INTERVAL '10 days' AND %(date)s
              AND bi.ranking <= %(ranking)s
            ORDER BY bi.provider_etf_id, bi.ticker_id, bi.value_date DESC, bi.ranking ASC
        )
        SELECT
            lpet.provider_etf_id,
            lpet.ticker_id,
            lpet.value_date,
            lpet.ranking,
            lpet.delta,
            t.style_type,
            t.exchange,
            t.country,
            t.esg_qualified,
            t.name,
            tv.market_cap
        FROM latest_per_etf_ticker lpet
        JOIN ticker t ON t.id = lpet.ticker_id
        LEFT JOIN LATERAL (
            SELECT market_cap
            FROM ticker_value
            WHERE ticker_id = lpet.ticker_id
              AND value_date BETWEEN %(date)s - INTERVAL '10 days' AND %(date)s
            ORDER BY value_date DESC
            LIMIT 1
        ) tv ON TRUE
        WHERE t.invalid IS NULL
    """
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, {'date': as_of_date, 'ranking': ranking_level})
                rows = cur.fetchall()
                cols = [desc[0] for desc in cur.description] if cur.description else []
        return pd.DataFrame(rows, columns=cols)
    except Error as e:
        raise Exception(f"Error fetching all best ideas as DataFrame: {e}")
                
