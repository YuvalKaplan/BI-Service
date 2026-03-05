from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Optional
from psycopg.errors import Error
from psycopg.rows import dict_row
from modules.core.db import db_pool_instance_bt

@dataclass
class TickerDividendHistory:
    symbol: str
    ex_date: date
    amount_per_share: Decimal
    id: Optional[int] = None

def fetch_dividends_for_holdings(account_id: int, eval_date: date):
    """
    Finds dividends for symbols held in the account as of the most recent snapshot.
    """
    try:
        with db_pool_instance_bt.get_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute("""
                    SELECT h.symbol, h.quantity, d.amount_per_share
                    FROM account_holding_daily h
                    JOIN ticker_dividend_history d ON h.symbol = d.symbol
                    WHERE h.account_id = %s 
                    AND h.holding_date = (
                        SELECT MAX(holding_date) FROM account_holding_daily 
                        WHERE account_id = %s AND holding_date < %s
                    )
                    AND d.ex_date = %s
                """, (account_id, account_id, eval_date, eval_date))
                return cur.fetchall()
    except Error as e:
            raise Exception(f"Error getting dividends of holdings: {e}")