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
    ranking: int
    appearances: int
    max_delta: float
    source_etf_id: int
    all_provider_ids: List[int]

def fetch_best_ideas_by_ranking(ranking_level: int, style_type: str, cap_type: str) -> List[BestIdeaRanked]:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(BestIdeaRanked)) as cur:
                cur.execute('SELECT * FROM get_best_ideas_by_ranking(%s, %s, %s);', (ranking_level, style_type, cap_type,))
                items = cur.fetchall()
        return items
    except Error as e:
        raise Exception(f"Error fetching the BestIdeaResult from the DB: {e}")
                
"""
The following is the SQL Function for get_best_ideas_by_ranking:
SELECT
	    be.symbol::TEXT,
	    be.ranking,
	    COUNT(DISTINCT be.provider_etf_id) AS appearances,
	    MAX(be.delta) AS max_delta,
	    (array_agg(be.provider_etf_id ORDER BY be.delta DESC))[1] AS source_etf_id,
	    array_agg(DISTINCT be.provider_etf_id) AS all_provider_ids
	
	FROM public.best_idea be
	JOIN public.ticker t ON be.symbol = t.symbol
	JOIN public.ticker_value tv ON tv.symbol = be.symbol
	JOIN (
	    SELECT symbol, MAX(value_date) AS max_date
	    FROM public.ticker_value
	    WHERE value_date >= CURRENT_DATE - INTERVAL '7 days'
	    GROUP BY symbol
	) latest_tv
	    ON tv.symbol = latest_tv.symbol
	   AND tv.value_date = latest_tv.max_date
	
	-- keep only the best ranking per symbol
	JOIN (
	    SELECT symbol, MIN(ranking) AS best_ranking
	    FROM public.best_idea
	    WHERE ranking <= p_ranking_level
	    GROUP BY symbol
	) best
	    ON be.symbol = best.symbol
	   AND be.ranking = best.best_ranking
	
	WHERE t.style_type = p_style_type
	  AND (
	      CASE
	          WHEN tv.market_cap >= 10000000000 THEN 'large'
	          ELSE 'mid_small'
	      END
	  ) = p_cap_type
	
	GROUP BY be.symbol, be.ranking
	ORDER BY be.ranking, appearances DESC, max_delta DESC;
"""