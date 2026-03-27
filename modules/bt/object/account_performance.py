from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import Optional, List
from psycopg.errors import Error
from modules.core.db import db_pool_instance_bt
from modules.bt.object import account_holding
from modules.bt.object import account_cash_ledger as acl

@dataclass
class AccountPerformance:
    account_id: int
    performance_date: date
    total_value: Decimal
    cash_balance: Decimal
    stock_value: Decimal
    daily_return: Decimal = Decimal('0')
    id: Optional[int] = None


def fetch_latest_total_value(account_id: int, prev_date: date) -> Decimal:
    try:
        with db_pool_instance_bt.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT total_value 
                    FROM account_performance_daily 
                    WHERE account_id = %s 
                      AND performance_date = (
                        SELECT MAX(performance_date) FROM account_performance_daily 
                        WHERE account_id = %s AND performance_date <= %s)
                """, (account_id, account_id, prev_date))
                res = cur.fetchone()
                return Decimal(res[0]) if res else Decimal('0')
    except Error as e:
        raise Exception(f"Error fetching previous TPV: {e}")


def record_daily_performance(account_id: int, eval_date: date, cash_balance: Decimal, snapshots: List[account_holding.AccountHolding]) -> Decimal:
    try:
        # 1. Calculate Today's Totals
        stock_value = sum(s.market_value for s in snapshots)
        tpv_today = cash_balance + stock_value

        # 2. Fetch Yesterday's TPV to calculate return
        yesterday = eval_date - timedelta(days=1)
        tpv_yesterday = fetch_latest_total_value(account_id, yesterday)

        # Daily Return = (Today / Yesterday) - 1
        # Handle the first day of backtest where yesterday is 0
        daily_ret = (tpv_today / tpv_yesterday) - 1 if tpv_yesterday > 0 else Decimal('0')

        # 3. Save to DB
        with db_pool_instance_bt.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO account_performance_daily 
                    (account_id, performance_date, total_value, cash_balance, stock_value, daily_return)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (account_id, performance_date) DO UPDATE SET
                        total_value = EXCLUDED.total_value,
                        daily_return = EXCLUDED.daily_return;
                """, (account_id, eval_date, tpv_today, cash_balance, stock_value, daily_ret))
            conn.commit()

        return daily_ret

    except Error as e:
        raise Exception(f"Error recording daily performance for the account: {e}")
