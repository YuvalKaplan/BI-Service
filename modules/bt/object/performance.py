import csv
import os
import re
import log
from datetime import date, timedelta
from decimal import Decimal
from dataclasses import dataclass
from psycopg.errors import Error
from psycopg.rows import class_row
from modules.core.db import db_pool_instance_bt


@dataclass
class AlphaAnnual:
    account_id: int
    benchmark_symbol: str
    performance_year: float
    annual_strategy_return: Decimal
    annual_benchmark_return: Decimal
    annual_alpha: Decimal


@dataclass
class DailyReturn:
    performance_date: date
    daily_return: Decimal


def fetch_alpha_annual() -> list[AlphaAnnual]:
    try:
        with db_pool_instance_bt.get_connection() as conn:
            with conn.cursor(row_factory=class_row(AlphaAnnual)) as cur:
                cur.execute('SELECT * FROM perf_get_alpha_annual();')
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error fetching annual alpha from DB: {e}")


def export_daily_returns_csv(account_id: int, file_name: str):
    try:
        with db_pool_instance_bt.get_connection() as conn:
            with conn.cursor(row_factory=class_row(DailyReturn)) as cur:
                cur.execute("""
                    SELECT performance_date, daily_return
                    FROM public.account_performance_daily
                    WHERE account_id = %s
                    ORDER BY performance_date;
                """, (account_id,))
                rows = cur.fetchall()

        supplemental_rows: list[tuple] = []
        if rows:
            first_date = rows[0].performance_date
            dec_31 = date(first_date.year - 1, 12, 31)
            if first_date != dec_31:
                current = dec_31
                while current < first_date:
                    supplemental_rows.append((current, Decimal(0)))
                    current += timedelta(days=1)

        safe_name = re.sub(r'[\\/:*?"<>|]', '-', file_name)
        os.makedirs(".output", exist_ok=True)
        with open(os.path.join(".output", f"{safe_name}.csv"), "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["date", "return"])
            for d, r in supplemental_rows:
                writer.writerow([d, r])
            for row in rows:
                writer.writerow([row.performance_date, row.daily_return])

        log.record_status(f"Exported {len(rows) + len(supplemental_rows)} daily return rows to '.output/{safe_name}.csv'.")

    except Error as e:
        raise Exception(f"Error exporting daily returns for account {account_id}: {e}")
