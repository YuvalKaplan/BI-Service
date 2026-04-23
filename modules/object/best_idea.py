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
                
"""
The following is the SQL Function for get_best_ideas_by_ranking.
TCREATE OR REPLACE FUNCTION public.get_best_ideas_by_ranking(
	p_ranking_level integer,
	p_style_type text,
	p_cap_type text,
	p_as_of_date date,
	p_provider_etf_ids integer[])
    RETURNS TABLE(symbol text, ranking integer, appearances bigint, max_delta double precision, source_etf_id integer, all_provider_ids integer[]) 
    LANGUAGE 'sql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$

	WITH constants AS (
	    SELECT INTERVAL '10 days' AS lookback
	),
	
	-- 1. Pre-filter the "Universe" of allowed symbols based on Style (and calculate cap level)
	eligible_symbols AS (
	    SELECT DISTINCT ON (t.symbol) 
	        t.symbol,
	        (CASE WHEN tv.market_cap >= 10000000000 THEN 'large' ELSE 'mid_small' END) as calc_cap
	    FROM public.ticker t
	    JOIN public.ticker_value tv ON t.symbol = tv.symbol, 
	    constants
	    WHERE tv.value_date <= p_as_of_date
	      AND tv.value_date >= p_as_of_date - lookback
	      -- Style Filter applied here
	      AND (p_style_type = 'blend' OR t.style_type = p_style_type)
	    ORDER BY t.symbol, tv.value_date DESC
	),
	
	-- 2. Further restrict by the Cap Filter
	filtered_universe AS (
	    SELECT symbol 
	    FROM eligible_symbols
	    WHERE (p_cap_type = 'all_cap' OR calc_cap = p_cap_type)
	),
	
	-- 3. Get the latest date
	symbol_latest_date AS (
	    SELECT 
	        be.symbol, 
	        MAX(be.value_date) as max_date
	    FROM public.best_idea be
	    JOIN filtered_universe fu ON be.symbol = fu.symbol,
	    constants
	    WHERE be.value_date <= p_as_of_date
	      AND be.value_date >= p_as_of_date - lookback
	      AND (cardinality(p_provider_etf_ids) = 0 OR be.provider_etf_id = ANY(p_provider_etf_ids))
	    GROUP BY be.symbol
	),

	-- 4. Get the best rank the max date
	symbol_targets AS (
	    SELECT DISTINCT ON (be.symbol)
	        be.symbol,
	        be.value_date AS max_date,
	        be.ranking AS best_ranking
	    FROM public.best_idea be
	    JOIN symbol_latest_date sld ON be.symbol = sld.symbol 
	                               AND be.value_date = sld.max_date
	    WHERE be.ranking <= p_ranking_level
	      AND (cardinality(p_provider_etf_ids) = 0 OR be.provider_etf_id = ANY(p_provider_etf_ids))
	    ORDER BY be.symbol, be.ranking ASC -- Takes the best rank available on the latest date
	)
	
	-- 5. Final aggregation using the pre-filtered targets
	SELECT
	    be.symbol::TEXT,
	    st.best_ranking AS ranking,
	    COUNT(DISTINCT be.provider_etf_id) AS appearances,
	    MAX(be.delta) AS max_delta,
	    (array_agg(be.provider_etf_id ORDER BY be.delta DESC))[1] AS source_etf_id,
	    array_agg(DISTINCT be.provider_etf_id) AS all_provider_ids
	
	FROM public.best_idea be
	JOIN symbol_targets st ON be.symbol = st.symbol 
	                       AND be.value_date = st.max_date 
	                       AND be.ranking = st.best_ranking
	GROUP BY be.symbol, st.best_ranking
	ORDER BY st.best_ranking, appearances DESC, max_delta DESC
	;

$BODY$;

ALTER FUNCTION public.get_best_ideas_by_ranking(integer, text, text, date, integer[])
    OWNER TO admin;
"""