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
    benchmark_mode: str = 'self'

def df_to_rows(
    best_ideas: pd.DataFrame,
    provider_etf_id: int,
    value_date: date,
    benchmark_mode: str = 'self',
) -> list[tuple]:
    rows = []
    for rank, (_, row) in enumerate(best_ideas.iterrows(), start=1):
        rows.append((
            provider_etf_id, int(row["ticker_id"]), value_date,
            float(row["etf_weight"]), float(row["benchmark_weight"]), float(row["delta"]),
            rank, benchmark_mode,
        ))
    return rows

def insert_bulk(provider_etf_id: int, value_date: date, benchmark_mode: str, rows: list[tuple]) -> None:
    """
    Replaces all best_idea rows for (provider_etf_id, value_date, benchmark_mode) with `rows`.
    Deletes first (even if `rows` is empty) so a rerun never leaves stale tickers behind
    that no longer qualify under the current ranking/eligibility logic.
    """
    query = """
        INSERT INTO best_idea
            (provider_etf_id, ticker_id, value_date, etf_weight, benchmark_weight, delta, ranking, benchmark_mode)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    'DELETE FROM best_idea WHERE provider_etf_id = %s AND value_date = %s AND benchmark_mode = %s',
                    (provider_etf_id, value_date, benchmark_mode),
                )
                if rows:
                    cur.executemany(query, rows)
    except Error as e:
        raise Exception(f"Error inserting Best Ideas in bulk: {e}")


def fetch_for_etf_date(provider_etf_id: int, value_date: date, benchmark_mode: str) -> List[BestIdea]:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(BestIdea)) as cur:
                cur.execute(
                    'SELECT * FROM best_idea WHERE provider_etf_id = %s AND value_date = %s AND benchmark_mode = %s ORDER BY ranking;',
                    (provider_etf_id, value_date, benchmark_mode),
                )
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error fetching best ideas for ETF {provider_etf_id} on {value_date}: {e}")


def fetch_for_etfs(etf_value_dates: dict[int, date], benchmark_mode: str) -> List[BestIdea]:
    """Best ideas of each {provider_etf_id: value_date} pair in one benchmark mode."""
    if not etf_value_dates:
        return []
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(BestIdea)) as cur:
                cur.execute(
                    """
                    SELECT bi.* FROM best_idea bi
                    JOIN unnest(%s::int[], %s::date[]) AS e(provider_etf_id, value_date)
                      ON e.provider_etf_id = bi.provider_etf_id AND e.value_date = bi.value_date
                    WHERE bi.benchmark_mode = %s
                    ORDER BY bi.provider_etf_id, bi.ranking;
                    """,
                    (list(etf_value_dates.keys()), list(etf_value_dates.values()), benchmark_mode),
                )
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error fetching best ideas for ETFs: {e}")


@dataclass
class BestIdeaRanked:
    ticker_id: int
    ranking: int
    appearances: int
    max_delta: float
    source_etf_id: int
    all_provider_ids: List[int]

def fetch_all_as_df(as_of_date: date) -> pd.DataFrame:
    """
    Load all best ideas within the lookback window for all benchmark modes, of the ETFs that are
    active now (an ETF that turned inactive stops feeding the funds at once), including ticker
    attributes needed for per-fund filtering.
    Returned DataFrame columns: provider_etf_id, ticker_id, value_date, ranking,
    delta, benchmark_mode, style_type, exchange, country, region, esg_qualified, name,
    master_ticker_id, market_cap, etf_region.
    market_cap is the listing's own latest cap within the window; funds_update.build_shared_context
    replaces it with the company's cap as of the date for multi-listing companies' masters.
    Callers should filter by benchmark_mode to match each fund's strategy.
    """
    sql = """
        WITH latest_date_per_etf AS (
            SELECT provider_etf_id, MAX(value_date) AS latest_date
            FROM best_idea
            WHERE value_date BETWEEN %(date)s - INTERVAL '10 days' AND %(date)s
            GROUP BY provider_etf_id
        ),
        latest_ideas AS (
            SELECT
                bi.provider_etf_id,
                bi.ticker_id,
                bi.value_date,
                bi.ranking,
                bi.delta,
                bi.benchmark_mode
            FROM best_idea bi
            JOIN latest_date_per_etf ld
                ON bi.provider_etf_id = ld.provider_etf_id
               AND bi.value_date = ld.latest_date
        )
        SELECT
            li.provider_etf_id,
            li.ticker_id,
            li.value_date,
            li.ranking,
            li.delta,
            li.benchmark_mode,
            t.style_type,
            t.exchange,
            t.country,
            t.region,
            t.esg_qualified,
            t.name,
            t.master_ticker_id,
            tv.market_cap,
            pe.region AS etf_region
        FROM latest_ideas li
        JOIN ticker t ON t.id = li.ticker_id
        JOIN provider_etf pe ON pe.id = li.provider_etf_id AND pe.status = 'active'
        LEFT JOIN LATERAL (
            SELECT market_cap
            FROM ticker_value
            WHERE ticker_id = li.ticker_id
              AND value_date BETWEEN %(date)s - INTERVAL '10 days' AND %(date)s
            ORDER BY value_date DESC
            LIMIT 1
        ) tv ON TRUE
        WHERE t.invalid IS NULL
        ORDER BY li.provider_etf_id, li.ranking ASC
    """
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, {'date': as_of_date})
                rows = cur.fetchall()
                cols = [desc[0] for desc in cur.description] if cur.description else []
        return pd.DataFrame(rows, columns=cols)
    except Error as e:
        raise Exception(f"Error fetching all best ideas as DataFrame: {e}")
                
