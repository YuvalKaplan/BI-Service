from datetime import date, datetime
from typing import List
from dataclasses import dataclass
from psycopg.errors import Error
from modules.core.db import db_pool_instance


@dataclass(kw_only=True)
class MarketBreakpoint:
    """
    The float cap at which a market's largest companies make up `coverage` of its total float cap,
    per the index funds' holdings on as_of_date (modules/ticker/index_funds.py). The benchmarks'
    cutoffs (benchmark.market_coverage), the funds' large-cap filter, the universe screener and the
    SEC ETF profiles' size classes read it by date.
    """
    as_of_date: date
    market: str               # 'US' | 'International'
    coverage: float           # 0.50 - 0.99
    created_at: datetime | None = None
    float_cap: float          # USD
    companies: int            # index companies at or above it


def replace_for_date(as_of_date: date, items: List[MarketBreakpoint]) -> None:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('DELETE FROM market_breakpoint WHERE as_of_date = %s;', (as_of_date,))
                cur.executemany(
                    'INSERT INTO market_breakpoint (as_of_date, market, coverage, float_cap, companies) VALUES (%s, %s, round(%s::numeric, 2), %s, %s);',
                    [(i.as_of_date, i.market, i.coverage, i.float_cap, i.companies) for i in items],
                )
            conn.commit()
    except Error as e:
        raise Exception(f"Error saving the market breakpoints for {as_of_date}: {e}")


def fetch_float_cap(market: str, coverage: float, as_of: date) -> tuple[date, float] | None:
    """(as_of_date, float_cap) of the market's breakpoint at this coverage from the latest date on or
    before as_of - else the earliest stored one (a date before breakpoints were kept) - else None."""
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                where = 'market = %s AND coverage = round(%s::numeric, 2)'
                cur.execute(f'SELECT as_of_date, float_cap FROM market_breakpoint WHERE {where} AND as_of_date <= %s '
                            'ORDER BY as_of_date DESC LIMIT 1;', (market, coverage, as_of))
                row = cur.fetchone()
                if row is None:
                    cur.execute(f'SELECT as_of_date, float_cap FROM market_breakpoint WHERE {where} ORDER BY as_of_date LIMIT 1;',
                                (market, coverage))
                    row = cur.fetchone()
                return (row[0], float(row[1])) if row else None
    except Error as e:
        raise Exception(f"Error fetching the {market} breakpoint at {coverage:.2f}: {e}")
