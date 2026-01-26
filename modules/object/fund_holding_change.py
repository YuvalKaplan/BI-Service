from datetime import date
from typing import List
from psycopg.errors import Error
from psycopg.rows import class_row
from dataclasses import dataclass
from modules.core.db import db_pool_instance
from modules.object.fund import Fund
import pandas as pd

@dataclass
class FundHoldingChange:
    fund_id: int
    symbol: str
    change_date: date
    direction: str
    ranking: int | None = None
    appearances: int | None = None
    max_delta: float | None = None
    top_delta_provider_etf_id: int | None = None
    all_provider_etf_ids: list[int] | None = None

def normalize_ids(ids: list[int] | None) -> list[int] | None:
    return ids if ids else None

def insert_fund_changes(items: List[FundHoldingChange]):
    if not items:
        return

    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:

                delete_sql = """
                    DELETE FROM fund_holding_change
                    WHERE fund_id = %s
                      AND symbol = %s
                      AND change_date = %s;
                """

                delete_values = [
                    (i.fund_id, i.symbol, i.change_date)
                    for i in items
                ]

                cur.executemany(delete_sql, delete_values)

                insert_sql = """
                    INSERT INTO fund_holding_change (
                        fund_id,
                        symbol,
                        change_date,
                        direction,
                        ranking,
                        appearances,
                        max_delta,
                        top_delta_provider_etf_id,
                        all_provider_etf_ids
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::integer[]);
                """

                insert_values = [
                    (
                        i.fund_id,
                        i.symbol,
                        i.change_date,
                        i.direction,
                        i.ranking,
                        i.appearances,
                        i.max_delta,
                        i.top_delta_provider_etf_id,
                        normalize_ids(i.all_provider_etf_ids),
                    )
                    for i in items
                ]

                cur.executemany(insert_sql, insert_values)

            conn.commit()

    except Error as e:
        raise Exception(f"Error inserting fund holding changes: {e}")
