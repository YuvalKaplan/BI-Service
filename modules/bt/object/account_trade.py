# account_trade.py
from dataclasses import dataclass
from decimal import Decimal
from datetime import date
from typing import Optional
from psycopg.errors import Error
from modules.core.db import db_pool_instance_bt

@dataclass
class AccountTrade:
    account_id: int
    symbol: str
    trade_date: date
    side: str  # 'BUY' or 'SELL'
    quantity: Decimal
    price: Decimal
    total_amount: Decimal # This will now be (Qty * Price) [+ Commission (for BUYS)] or [- Commission (for (SELLS)]
    commission: Decimal = Decimal('0.00')
    id: Optional[int] = None

def record_trade(trade: AccountTrade) -> None:
    try:
        with db_pool_instance_bt.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO account_trade (account_id, symbol, trade_date, side, quantity, price, commission,  total_amount)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (trade.account_id, trade.symbol, trade.trade_date, trade.side, 
                    trade.quantity, trade.price, trade.commission, trade.total_amount))

    except Error as e:
        raise Exception(f"Error insertinf account trades into the DB: {e}")