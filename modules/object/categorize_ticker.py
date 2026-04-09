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
    factors: Dict[str, Any]
    last_update: Optional[datetime] = None

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


def fetch_all() -> List[CategorizeTicker]:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute("""
                    SELECT symbol, style_type, cap_type, sector, market_cap, factors, last_update
                    FROM categorize_ticker
                """)
                rows = cur.fetchall()
                result = []
                for r in rows:
                    factors = r["factors"]
                    if isinstance(factors, str):
                        factors = json.loads(factors)
                    result.append(CategorizeTicker(
                        symbol=r["symbol"],
                        style_type=r["style_type"],
                        cap_type=r["cap_type"],
                        sector=r["sector"],
                        market_cap=r["market_cap"],
                        factors=factors,
                        last_update=r.get("last_update")
                    ))
                return result

    except Error as e:
        raise Exception(f"Error loading categorize ticker: {e}")


def fetch_symbols() -> list[str]:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT DISTINCT symbol FROM categorize_ticker ORDER BY symbol")
                return [row[0] for row in cur.fetchall()]

    except Error as e:
        raise Exception(f"Error loading categorize ticker symbols: {e}")
