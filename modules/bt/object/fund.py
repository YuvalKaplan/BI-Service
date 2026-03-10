from datetime import datetime
from typing import Dict, Optional
from pydantic import BaseModel
from psycopg.errors import Error
from psycopg.rows import class_row
from dataclasses import dataclass
from modules.core.db import db_pool_instance_bt

@dataclass
class Fund:
    id: int
    created_at: datetime 
    name: str
    strategy: dict

class Cap(BaseModel):
    name: str    

class Style(BaseModel):
    name: str
    value: Optional[int] = None
    growth: Optional[int] = None
    
class Strategy(BaseModel):
    holdings: int
    cap: Cap
    style: Style
    thematic: Optional[str] = None
    benchmarks: Optional[list[str]] = None
    provider_etfs: Optional[list[int]] = None

def getStrategyFromJson(data: dict) -> Strategy:
    return Strategy.model_validate(data)

def fetch_all():
    try:
        with db_pool_instance_bt.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Fund)) as cur:
                query_str = "SELECT * FROM fund;"
                cur.execute(query_str)
                items = cur.fetchall()
        return items
    except Error as e:
        raise Exception(f"Error fetching the Fund list from the DB: {e}")

def fetch_fund(fund_id: int):
    try:
        with db_pool_instance_bt.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Fund)) as cur:
                cur.execute("""
                    SELECT * FROM fund 
                    WHERE id = %s 
                """, (fund_id,))
                items = cur.fetchone()
        return items
    except Error as e:
        raise Exception(f"Error fetching the Fund from the DB: {e}")
