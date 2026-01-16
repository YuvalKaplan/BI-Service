from datetime import datetime
from psycopg.errors import Error
from psycopg.rows import class_row
from dataclasses import dataclass
from modules.core.db import db_pool_instance
import pandas as pd

@dataclass
class ProviderEtfHolding:
    id: int
    created_at: datetime
    fund_id: int
    ticker: str
    sources: dict | None
    weight: float | None

def insert_all_holdings(fund_id: int, df: pd.DataFrame):
    try:
        df = df.drop(columns=["id"], errors="ignore")
        df["fund_id"] = fund_id
        df = df[[
            "fund_id",
            "ticker",
            "sources",
            "weight"
        ]]
        rows = list(df.itertuples(index=False, name=None)).copy()

        #  Delete the Holdings data for a ETF on a Trade Date to prevent duplicates
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                delete_query = """
                    DELETE FROM fund_holding peh
                    WHERE peh.fund_id = %s 
                    AND peh.created_at = %s;
                """
                cur.execute(delete_query, (fund_id, df["trade_date"].iat[0]))    

        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                insert_query = "INSERT INTO fund_holding (fund_id, trade_date, ticker, weight) VALUES (%s, %s, %s, %s, %s, %s);"
                cur.executemany(insert_query, rows)

    except Error as e:
        raise Exception(f"Error inserting the Provider ETF Holdings into the DB: {e}")
    
