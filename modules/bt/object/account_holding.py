from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import List
from psycopg.errors import Error
from typing import Optional
from psycopg.rows import class_row
from dataclasses import dataclass
from modules.core.db import db_pool_instance_bt

@dataclass
class AccountHolding:
    account_id: int
    holding_date: date
    symbol: str
    quantity: Decimal
    cost_basis: Decimal
    market_value: Decimal = Decimal(0)
    weight_percentage: Decimal = Decimal(0)
    id: Optional[int] = None

def fetch_current_account_snapshot(account_id: int, eval_date: date):
    try:
        with db_pool_instance_bt.get_connection() as conn:
            with conn.cursor(row_factory=class_row(AccountHolding)) as cur:
                cur.execute("""
                    SELECT * FROM account_holding_daily 
                    WHERE account_id = %s 
                    AND holding_date = (
                        SELECT MAX(holding_date) FROM account_holding_daily 
                        WHERE account_id = %s AND holding_date < %s
                    )
                """, (account_id, account_id, eval_date))
                items = cur.fetchall()
        return items
    except Error as e:
        raise Exception(f"Error fetching the Currenct Account Snapshot from the DB: {e}")

def record_account_holdings(holdings: List[AccountHolding]):
    if not holdings:
        return

    # Prepare data into a list of tuples for the bulk call
    params = [
        (h.account_id, h.holding_date, h.symbol, h.quantity, 
         h.cost_basis, h.market_value, h.weight_percentage) 
        for h in holdings
    ]

    query = """
        INSERT INTO account_holding_daily 
        (account_id, holding_date, symbol, quantity, cost_basis, market_value, weight_percentage)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (account_id, holding_date, symbol) 
        DO UPDATE SET
            quantity = EXCLUDED.quantity,
            cost_basis = EXCLUDED.cost_basis,
            market_value = EXCLUDED.market_value,
            weight_percentage = EXCLUDED.weight_percentage;
    """

    try:
        with db_pool_instance_bt.get_connection() as conn:
            with conn.cursor() as cur:
                # This performs a high-speed bulk insert in one "trip"
                cur.executemany(query, params)
            conn.commit()
    except Exception as e:
        raise Exception(f"Error saving the latest account holdings snapshot into the DB: {e}")
