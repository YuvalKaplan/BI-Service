from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Optional
from psycopg.errors import Error
from modules.core.db import db_pool_instance_bt

@dataclass
class AccountCashLedger:
    account_id: int
    transaction_date: date
    amount: Decimal
    entry_type: str
    description: Optional[str] = None
    id: Optional[int] = None

def get_cash_balance(account_id: int, eval_date: date) -> Decimal:
    try:
        with db_pool_instance_bt.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT SUM(amount) FROM account_cash_ledger 
                    WHERE account_id = %s AND transaction_date < %s
                """, (account_id, eval_date))
                res = cur.fetchone()
                # Handle None if table is empty
                return Decimal(res[0]) if res and res[0] is not None else Decimal('0.00')
    except Error as e:
        raise Exception(f"Error getting cash balance: {e}")

def record_cash_transaction(entry: AccountCashLedger) -> None:
    try:
        with db_pool_instance_bt.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO account_cash_ledger (account_id, transaction_date, amount, entry_type, description)
                    VALUES (%s, %s, %s, %s, %s)
                """, (entry.account_id, entry.transaction_date, entry.amount, entry.entry_type, entry.description))
    except Error as e:
        raise Exception(f"Error recording cash transaction: {e}")
