from datetime import date
from typing import List
from psycopg.errors import Error
from psycopg.rows import class_row
from dataclasses import dataclass
from modules.core.db import db_pool_instance
from modules.object.fund import Fund
import pandas as pd

@dataclass
class FundHolding:
    fund_id: int
    symbol: str
    holding_date: date
    ranking: int 

def fetch_funds_holdings(fund_id: int):
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(FundHolding)) as cur:
                query_str = """
                    SELECT * FROM fund_holding
                    WHERE fund_id = %s
                      AND holding_date = (
                          SELECT MAX(holding_date)
                          FROM fund_holding
                          WHERE fund_id = %s 
                            AND holding_date < CURRENT_DATE
                      );
                """
                cur.execute(query_str, (fund_id, fund_id,))
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
        with db_pool_instance.get_connection() as conn:
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
                        ranking
                    )
                    VALUES (%s, %s, %s, %s);
                """

                insert_values = [
                    (i.fund_id, i.symbol, i.holding_date, i.ranking)
                    for i in items
                ]

                cur.executemany(insert_sql, insert_values)

            conn.commit()

    except Error as e:
        raise Exception(f"Error replacing fund holdings in DB: {e}")