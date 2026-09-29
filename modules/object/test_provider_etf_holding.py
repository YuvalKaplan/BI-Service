from datetime import datetime
from typing import List
from dataclasses import dataclass
from psycopg import sql
from psycopg.errors import Error
from modules.core.db import db_pool_instance


@dataclass(kw_only=True)
class TestProviderEtfHolding:
    """
    One holding line of a test provider ETF, as FMP lists it - parallel to provider_etf_holding.
    ticker_id is set when the line matches a ticker we already have (none are registered for it),
    so FMP's symbol / name / ISIN / CUSIP are kept to identify the rest. weight is a fraction.
    """
    id: int | None = None
    created_at: datetime | None = None
    test_provider_etf_id: int
    holding_date: datetime
    ticker_id: int | None = None
    shares: float | None = None
    market_value: float | None = None
    weight: float | None = None
    symbol: str | None = None
    name: str | None = None
    isin: str | None = None
    cusip: str | None = None


_COLUMNS = ('test_provider_etf_id', 'holding_date', 'ticker_id', 'shares', 'market_value', 'weight',
            'symbol', 'name', 'isin', 'cusip')
_INSERT_SQL = sql.SQL("INSERT INTO test_provider_etf_holding ({columns}) VALUES ({placeholders});").format(
    columns=sql.SQL(", ").join(map(sql.Identifier, _COLUMNS)),
    placeholders=sql.SQL(", ").join(sql.Placeholder() for _ in _COLUMNS),
)


def fetch_etf_ids_with_holdings() -> set[int]:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT DISTINCT test_provider_etf_id FROM test_provider_etf_holding;')
                return {row[0] for row in cur.fetchall()}
    except Error as e:
        raise Exception(f"Error fetching the test provider ETFs with holdings: {e}")


def insert_all(items: List[TestProviderEtfHolding]) -> None:
    if not items:
        return
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(_INSERT_SQL, [tuple(getattr(i, c) for c in _COLUMNS) for i in items])
            conn.commit()
    except Error as e:
        raise Exception(f"Error saving the holdings of test provider ETF {items[0].test_provider_etf_id}: {e}")
