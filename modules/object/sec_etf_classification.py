from datetime import datetime
from dataclasses import dataclass
from psycopg import sql
from psycopg.errors import Error
from psycopg.rows import class_row
from modules.core.db import db_pool_instance


@dataclass(kw_only=True)
class SecEtfClassification:
    """
    What modules/sec/etf_profile.py made of an SEC active ETF from its FMP data: its strategy
    (equity, option_income, buffer, fixed_income, ...) and the holdings shares behind it, or why
    FMP couldn't tell (fmp_error). Every checked fund has one, so a fund left out of the test
    provider tables isn't checked again before its next refresh.
    """
    series_id: str
    created_at: datetime | None = None
    updated_at: datetime | None = None
    fmp_error: str | None = None
    asset_class: str | None = None       # FMP etf/info assetClass
    strategy: str | None = None
    equity_weight: float | None = None   # share of the fund in stock lines
    stock_weight: float | None = None    # share matched to the index funds' stocks


_WRITE_COLUMNS = ('series_id', 'fmp_error', 'asset_class', 'strategy', 'equity_weight', 'stock_weight')
_UPSERT_SQL = sql.SQL(
    "INSERT INTO sec_etf_classification ({columns}) VALUES ({placeholders}) "
    "ON CONFLICT (series_id) DO UPDATE SET {updates}, updated_at = (now() AT TIME ZONE 'utc');"
).format(
    columns=sql.SQL(", ").join(map(sql.Identifier, _WRITE_COLUMNS)),
    placeholders=sql.SQL(", ").join(sql.Placeholder() for _ in _WRITE_COLUMNS),
    updates=sql.SQL(", ").join(
        sql.SQL("{c} = EXCLUDED.{c}").format(c=sql.Identifier(c)) for c in _WRITE_COLUMNS if c != 'series_id'
    ),
)


def fetch_all() -> dict[str, SecEtfClassification]:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(SecEtfClassification)) as cur:
                cur.execute('SELECT * FROM sec_etf_classification;')
                return {c.series_id: c for c in cur.fetchall()}
    except Error as e:
        raise Exception(f"Error fetching the SEC ETF classifications: {e}")


def upsert(item: SecEtfClassification) -> None:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(_UPSERT_SQL, tuple(getattr(item, c) for c in _WRITE_COLUMNS))
            conn.commit()
    except Error as e:
        raise Exception(f"Error saving the classification of {item.series_id}: {e}")
