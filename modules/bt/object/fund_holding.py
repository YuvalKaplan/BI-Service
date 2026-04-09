from datetime import date
from typing import List
from psycopg.errors import Error
from psycopg.rows import class_row
from modules.core.db import db_pool_instance_bt
from modules.calc.model_fund import FundHolding  # noqa: F401

def fetch_funds_holdings(fund_id: int, eval_date: date):
    try:
        with db_pool_instance_bt.get_connection() as conn:
            with conn.cursor(row_factory=class_row(FundHolding)) as cur:
                cur.execute("""
                    SELECT * FROM fund_holding 
                    WHERE fund_id = %s 
                      AND holding_date = (
                        SELECT MAX(holding_date) FROM fund_holding 
                        WHERE fund_id = %s AND holding_date <= %s);
                """, (fund_id, fund_id, eval_date))
                items = cur.fetchall()
        return items
    except Error as e:
        raise Exception(f"Error fetching the Fund Holdings from the DB: {e}")


def insert_fund_holding(items: List[FundHolding]):
    if not items:
        return
    
    fund_id = items[0].fund_id
    holding_date = items[0].holding_date

    try:
        with db_pool_instance_bt.get_connection() as conn:
            with conn.cursor() as cur:

                delete_sql = """
                    DELETE FROM fund_holding
                    WHERE fund_id = %s
                      AND holding_date = %s;
                """
                cur.execute(delete_sql, (fund_id, holding_date))

                insert_sql = """
                    INSERT INTO fund_holding (
                        fund_id,
                        symbol,
                        holding_date,
                        ranking,
                        source_etf_id,
                        max_delta
                    )
                    VALUES (%s, %s, %s, %s, %s, %s);
                """

                insert_values = [
                    (i.fund_id, i.symbol, i.holding_date, i.ranking, i.source_etf_id, i.max_delta)
                    for i in items
                ]

                cur.executemany(insert_sql, insert_values)

            conn.commit()

    except Error as e:
        raise Exception(f"Error replacing fund holdings in DB: {e}")