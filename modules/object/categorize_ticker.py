from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Dict, Any
from psycopg.errors import Error
from psycopg.rows import dict_row
from modules.core.db import db_pool_instance
import json


@dataclass
class CategorizeTicker:
    symbol: str
    style_type: str | None
    cap_type: str | None
    sector: str
    market_cap: int
    esg_qualified: bool | None
    factors: Dict[str, Any]
    last_update: Optional[datetime] = None

def _row_to_categorize_ticker(r: dict) -> CategorizeTicker:
    factors = r["factors"]
    if isinstance(factors, str):
        factors = json.loads(factors)
    return CategorizeTicker(
        symbol=r["symbol"],
        style_type=r["style_type"],
        cap_type=r["cap_type"],
        sector=r["sector"],
        market_cap=r["market_cap"],
        esg_qualified=r.get("esg_qualified"),
        factors=factors,
        last_update=r.get("last_update")
    )


def sync_categorize_ticker():
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT public.sync_categorize_ticker();")
        
    except Error as e:
        raise Exception(f"Error executing stored procedure: {e}")

def update(symbol: str, sector: str, market_cap: int, factors: Dict[str, Any]):
    if not symbol and not factors:
        return

    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE categorize_ticker
                    SET sector = %s, market_cap = %s, factors = %s
                    WHERE symbol = %s
                """, (sector, market_cap, json.dumps(factors), symbol))
            conn.commit()

    except Error as e:
        raise Exception(f"Error updating categorize ticker: {e}")


def bulk_update(updates: list[dict]):
    if not updates:
        return

    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.executemany("""
                    UPDATE categorize_ticker
                    SET sector = %s, market_cap = %s, factors = %s
                    WHERE symbol = %s
                """, [(u["sector"], u["market_cap"], json.dumps(u["factors"]), u["symbol"]) for u in updates])
            conn.commit()

    except Error as e:
        raise Exception(f"Error bulk updating categorize ticker: {e}")


def update_esg_qualified(symbols: list[str]):
    if not symbols:
        return
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE categorize_ticker
                    SET esg_qualified = TRUE
                    WHERE symbol = ANY(%s::text[]);
                """, (symbols,))
            conn.commit()
    except Error as e:
        raise Exception(f"Error updating esg_qualified in categorize_ticker: {e}")


def fetch_all_for_style_classification() -> List[CategorizeTicker]:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute("""
                    SELECT symbol, style_type, cap_type, sector, market_cap, esg_qualified, factors, last_update
                    FROM categorize_ticker
                    WHERE style_type IS NOT NULL
                      AND factors IS NOT NULL
                      AND factors != '{}'::jsonb
                """)
                rows = cur.fetchall()
                return [_row_to_categorize_ticker(r) for r in rows]

    except Error as e:
        raise Exception(f"Error loading categorize ticker: {e}")


def fetch_all_for_esg() -> List[CategorizeTicker]:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute("""
                    SELECT symbol, style_type, cap_type, sector, market_cap, esg_qualified, factors, last_update
                    FROM categorize_ticker
                    WHERE esg_qualified = TRUE
                """)
                return [_row_to_categorize_ticker(r) for r in cur.fetchall()]

    except Error as e:
        raise Exception(f"Error loading ESG categorize tickers: {e}")


def fetch_last_update() -> datetime | None:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT MAX(last_update) FROM categorize_ticker")
                result = cur.fetchone()
                return result[0] if result else None

    except Error as e:
        raise Exception(f"Error fetching categorize_ticker last update: {e}")


def fetch_symbols() -> list[str]:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT DISTINCT symbol FROM categorize_ticker ORDER BY symbol")
                return [row[0] for row in cur.fetchall()]

    except Error as e:
        raise Exception(f"Error loading categorize ticker symbols: {e}")
