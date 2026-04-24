import json
from dataclasses import dataclass
from datetime import datetime
from typing import List
from psycopg.errors import Error
from psycopg.rows import class_row
from modules.core.db import db_pool_instance


@dataclass
class CategorizeTicker:
    id:          int
    name:        str | None = None
    symbol:      str | None = None
    isin:        str | None = None
    exchange:    str | None = None
    country:     str | None = None
    currency:    str | None = None
    style_type:  str | None = None
    cap_type:    str | None = None
    sector:      str | None = None
    market_cap:  int | None = None
    factors:     dict | None = None
    last_update: datetime | None = None


def fetch_all_for_style_classification() -> List[CategorizeTicker]:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(CategorizeTicker)) as cur:
                cur.execute("""
                    SELECT id, name, symbol, isin, exchange, country, currency,
                           style_type, cap_type, sector, market_cap, factors, last_update
                    FROM categorize_ticker
                    WHERE style_type IS NOT NULL
                      AND factors IS NOT NULL
                      AND factors != '{}'::jsonb
                """)
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error loading categorize_ticker for style classification: {e}")

def upsert(item: dict) -> int | None:
    """Insert or update a single categorize_ticker row. Conflict key: (symbol, exchange). Returns the assigned id."""
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO categorize_ticker
                        (name, symbol, isin, exchange, country, currency,
                         style_type, cap_type, sector, market_cap, factors, last_update)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                    ON CONFLICT (symbol, exchange)
                    DO UPDATE SET
                        name        = EXCLUDED.name,
                        isin        = EXCLUDED.isin,
                        country     = EXCLUDED.country,
                        currency    = EXCLUDED.currency,
                        style_type  = EXCLUDED.style_type,
                        cap_type    = EXCLUDED.cap_type,
                        sector      = EXCLUDED.sector,
                        market_cap  = EXCLUDED.market_cap,
                        factors     = EXCLUDED.factors,
                        last_update = now()
                    RETURNING id
                """, (
                    item.get("name"),
                    item.get("symbol"),
                    item.get("isin"),
                    item.get("exchange"),
                    item.get("country"),
                    item.get("currency"),
                    item.get("style_type"),
                    item.get("cap_type"),
                    item.get("sector"),
                    item.get("market_cap"),
                    json.dumps(item["factors"]) if item.get("factors") else None,
                ))
                row = cur.fetchone()
                return row[0] if row else None
    except Error as e:
        raise Exception(f"Error upserting categorize_ticker row: {e}")

