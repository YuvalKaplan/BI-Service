from datetime import date
from typing import List
from psycopg.errors import Error
from psycopg.rows import class_row
from modules.core.db import db_pool_instance_bt
from modules.bt.calc.model_fund import FundHoldingChange  # noqa: F401

def normalize_ids(ids: list[int] | None) -> list[int] | None:
    return ids if ids else None

def insert_fund_changes(items: List[FundHoldingChange]) -> None:
    if not items:
        return
    
    fund_id = items[0].fund_id
    change_date = items[0].change_date

    try:
        with db_pool_instance_bt.get_connection() as conn:
            with conn.cursor() as cur:

                # Delete once for this fund & date
                delete_sql = """
                    DELETE FROM fund_holding_change
                    WHERE fund_id = %s
                      AND change_date = %s;
                """
                cur.execute(delete_sql, (fund_id, change_date))

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
                        all_provider_etf_ids,
                        reason
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::integer[], %s);
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
                        i.reason
                    )
                    for i in items
                ]

                cur.executemany(insert_sql, insert_values)

            conn.commit()

    except Error as e:
        raise Exception(f"Error inserting fund holding changes: {e}")
