from datetime import date, datetime, timezone
from psycopg.errors import Error
from psycopg.rows import class_row
from dataclasses import dataclass
from modules.core.db import db_pool_instance_bt
from modules.object.provider import Provider 


@dataclass
class ProviderEtf:
    id: int
    created_at: datetime | None
    provider_id: int | None
    disabled: bool | None
    disabled_reason: str | None
    name: str | None
    description: str | None
    isin: str | None
    ticker: str | None
    cap_type: str | None
    style_type: str | None
    benchmark: str | None
    trading_since: datetime | None
    number_of_managers: int | None
    url: str | None
    file_format: str | None

def fetch_by_id(id: int) -> ProviderEtf:
    with db_pool_instance_bt.get_connection() as conn:
        with conn.cursor(row_factory=class_row(ProviderEtf)) as cur:
            cur.execute('SELECT * FROM provider_etf WHERE id = %s;', (id,))
            item = cur.fetchone()
        if item is None:
                raise Exception(f"Provider ETF not found for ID {id}")
    return item

def fetch_by_ticker(ticker: str) -> ProviderEtf:
    with db_pool_instance_bt.get_connection() as conn:
        with conn.cursor(row_factory=class_row(ProviderEtf)) as cur:
            cur.execute('SELECT * FROM provider_etf WHERE ticker = %s;', (ticker,))
            item = cur.fetchone()
        if item is None:
            raise Exception(f"Provider ETF not found for ID {id}")
    return item

def fetch_by_provider_id(provider_id: int):
    try:
        with db_pool_instance_bt.get_connection() as conn:
            with conn.cursor(row_factory=class_row(ProviderEtf)) as cur:
                query_str = """
                    SELECT * FROM provider_etf
                    WHERE provider_id = %s
                    AND NOT disabled
                    ORDER BY created_at;
                """
                cur.execute(query_str, (provider_id,))
                items = cur.fetchall()
        return items
    except Error as e:
        raise Exception(f"Error fetching the Provider ETFs for provider ID from the DB: {e}")

