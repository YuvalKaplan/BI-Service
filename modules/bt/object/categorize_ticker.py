from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Dict, Any
from psycopg.errors import Error
from psycopg.rows import dict_row
from modules.core.db import db_pool_instance_bt
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

def fetch_all_for_style_classification() -> List[CategorizeTicker]:
    try:
        with db_pool_instance_bt.get_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute("""
                    SELECT symbol, style_type, cap_type, sector, market_cap, esg_qualified, factors, last_update
                    FROM categorize_ticker
                    WHERE style_type IS NOT NULL
                      AND factors IS NOT NULL
                      AND factors != '{}'::jsonb
                """)
                return [_row_to_categorize_ticker(r) for r in cur.fetchall()]

    except Error as e:
        raise Exception(f"Error loading categorize ticker: {e}")


def fetch_all_for_esg() -> List[CategorizeTicker]:
    try:
        with db_pool_instance_bt.get_connection() as conn:
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
        with db_pool_instance_bt.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT MAX(last_update) FROM categorize_ticker")
                result = cur.fetchone()
                return result[0] if result else None

    except Error as e:
        raise Exception(f"Error fetching categorize_ticker last update: {e}")

