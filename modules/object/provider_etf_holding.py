from typing import List
from datetime import date, datetime
from psycopg.errors import Error
from psycopg.rows import class_row
from dataclasses import dataclass
from modules.core.db import db_pool_instance
import pandas as pd

@dataclass
class ProviderEtfHolding:
    id: int
    created_at: datetime
    provider_etf_id: int
    trade_date: date
    ticker: str
    shares: float
    market_value: float
    weight: float

def fetch_holding_dates_available_past_week(provider_etf_id: int) -> list[date]:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                query = """
                    SELECT DISTINCT (trade_date::date)
                        trade_date
                    FROM provider_etf_holding
                    WHERE provider_etf_id = %s 
                      AND trade_date > NOW() - INTERVAL '1 week'
                    ORDER BY trade_date::date;
                """
                cur.execute(query, (provider_etf_id,))
                return [row[0] for row in cur.fetchall()]
    except Error as e:
        raise Exception(f"Error retrieving the list of holding dates available in the past week: {e}")
    
def fetch_valid_tickers_in_holdings() -> List[str]:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                query = """
                    SELECT DISTINCT peh.ticker 
                    FROM public.provider_etf_holding AS peh
                    INNER JOIN ticker AS t ON peh.ticker = t.symbol
                    WHERE t.invalid IS null;
                """
                cur.execute(query)
                return [row[0] for row in cur.fetchall()]
    except Error as e:
        raise Exception(f"Error retrieving TickerMarketCap data: {e}")

def fetch_valid_holdings_by_provider_etf_id(provider_etf_id: int, trade_date: date):
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(ProviderEtfHolding)) as cur:
                query_str = """
                    SELECT peh.*
                    FROM provider_etf_holding AS peh
                    INNER JOIN ticker AS t ON peh.ticker = t.symbol
                    WHERE t.invalid IS null
                      AND provider_etf_id = %s
                      AND trade_date = %s;
                """
                cur.execute(query_str, (provider_etf_id, trade_date))
                items = cur.fetchall()
        return items
    except Error as e:
        raise Exception(f"Error fetching the Provider ETFs Holdings for provider ETF ID from the DB: {e}")


def insert_all_holdings(etf_id: int, df: pd.DataFrame):
    try:
        df = df.drop(columns=["id"], errors="ignore")
        df["provider_etf_id"] = etf_id
        df = df[[
            "provider_etf_id",
            "trade_date",
            "ticker",
            "shares",
            "market_value",
            "weight"
        ]]
        rows = list(df.itertuples(index=False, name=None)).copy()

        #  Delete the Holdings data for a ETF on a Trade Date to prevent duplicates
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                delete_query = """
                    DELETE FROM provider_etf_holding peh
                    WHERE peh.provider_etf_id = %s 
                    AND peh.trade_date = %s;
                """
                cur.execute(delete_query, (etf_id, df["trade_date"].iat[0]))    

        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                insert_query = "INSERT INTO provider_etf_holding (provider_etf_id, trade_date, ticker, shares, market_value, weight) VALUES (%s, %s, %s, %s, %s, %s);"
                cur.executemany(insert_query, rows)

    except Error as e:
        raise Exception(f"Error inserting the Provider ETF Holdings into the DB: {e}")
    
