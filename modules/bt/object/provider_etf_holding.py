from typing import List, Optional
from datetime import date, datetime
from psycopg.errors import Error
from psycopg.rows import class_row
from dataclasses import dataclass
from modules.core.db import db_pool_instance_bt
import pandas as pd

@dataclass
class ProviderEtfHolding:
    provider_etf_id: int
    trade_date: date
    ticker: str
    shares: float
    market_value: float
    weight: float
    id: Optional[int] = None
    created_at: Optional[datetime] = None

def fetch_holding_dates_available_past_week(provider_etf_id: int, end_date: date) -> list[date]:
    try:
        with db_pool_instance_bt.get_connection() as conn:
            with conn.cursor() as cur:
                query = """
                    SELECT DISTINCT (peh.trade_date::date)
                    FROM provider_etf_holding AS peh
                    INNER JOIN ticker AS t ON peh.ticker = t.symbol
                    WHERE t.invalid IS null
                      AND peh.provider_etf_id = %s 
                      AND (trade_date > %s - INTERVAL '1 week') AND (trade_date <= %s)
                    ORDER BY peh.trade_date::date;
                """
                cur.execute(query, (provider_etf_id, end_date, end_date,))
                return [row[0] for row in cur.fetchall()]
    except Error as e:
        raise Exception(f"Error retrieving the list of holding dates available in the past week: {e}")
    
def fetch_valid_tickers_in_holdings() -> List[str]:
    try:
        with db_pool_instance_bt.get_connection() as conn:
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
        raise Exception(f"Error retrieving ticker list that are valid: {e}")

def fetch_tickers_for_etfs(provider_etf_ids: List[int]) -> List[str]:
    try:
        with db_pool_instance_bt.get_connection() as conn:
            with conn.cursor() as cur:
                query = """
                    SELECT DISTINCT peh.ticker 
                    FROM public.provider_etf_holding AS peh
                    WHERE peh.provider_etf_id = ANY(%s);
                """
                cur.execute(query, (provider_etf_ids,))
                return [row[0] for row in cur.fetchall()]
    except Error as e:
        raise Exception(f"Error retrieving ticker list for a list of provider ETFs data: {e}")


def fetch_valid_holdings_by_provider_etf_id(provider_etf_id: int, trade_date: date):
    try:
        with db_pool_instance_bt.get_connection() as conn:
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


def insert_holding_bulk(items: List[ProviderEtfHolding]):
    if not items:
        return
    
    fund_id = items[0].provider_etf_id
    trade_date = items[0].trade_date

    try:
        with db_pool_instance_bt.get_connection() as conn:
            with conn.cursor() as cur:

                delete_sql = """
                    DELETE FROM provider_etf_holding
                    WHERE provider_etf_id = %s
                      AND trade_date = %s;
                """
                cur.execute(delete_sql, (fund_id, trade_date))

                insert_sql = """
                    INSERT INTO provider_etf_holding (
                        provider_etf_id,
                        trade_date,
                        ticker,
                        shares,
                        market_value,
                        weight
                    )
                    VALUES (%s, %s, %s, %s, %s, %s);
                """

                insert_values = [
                    (i.provider_etf_id, i.trade_date, i.ticker, i.shares, i.market_value, i.weight)
                    for i in items
                ]

                cur.executemany(insert_sql, insert_values)

            conn.commit()

    except Error as e:
        raise Exception(f"Error replacing fund holdings in DB: {e}")
    