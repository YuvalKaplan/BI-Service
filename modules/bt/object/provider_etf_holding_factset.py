from typing import List, Optional
from datetime import date, datetime
from psycopg.errors import Error
from psycopg.rows import class_row
from dataclasses import dataclass
from modules.core.db import db_pool_instance_bt
import pandas as pd

@dataclass
class ProviderEtfHoldingFactSet:
    provider_etf_id: int
    holding_date: date
    ticker: str
    shares: float
    market_value: Optional[float] = None
    weight: Optional[float] = None
    id: Optional[int] = None
    created_at: Optional[date] = None



def insert_holding_bulk(items: List[ProviderEtfHoldingFactSet]) -> None:
    if not items:
        return
    
    fund_id = items[0].provider_etf_id
    holding_date = items[0].holding_date

    try:
        with db_pool_instance_bt.get_connection() as conn:
            with conn.cursor() as cur:

                delete_sql = """
                    DELETE FROM provider_etf_holding_factset
                    WHERE provider_etf_id = %s
                      AND holding_date = %s;
                """
                cur.execute(delete_sql, (fund_id, holding_date))

                insert_sql = """
                    INSERT INTO provider_etf_holding_factset (
                        created_at,
                        provider_etf_id,
                        holding_date,
                        ticker,
                        shares,
                        market_value,
                        weight
                    )
                    VALUES (%s, %s,  %s, %s, %s, %s, %s);
                """

                insert_values = [
                    (i.created_at, i.provider_etf_id, i.holding_date, i.ticker, i.shares, i.market_value, i.weight)
                    for i in items
                ]

                cur.executemany(insert_sql, insert_values)

            conn.commit()

    except Error as e:
        raise Exception(f"Error replacing fund holdings in DB: {e}")
    