from datetime import datetime
from dataclasses import dataclass
from psycopg import sql
from psycopg.errors import Error
from psycopg.rows import class_row
from psycopg.types.json import Jsonb
from modules.core.db import db_pool_instance

PENDING, ACTIVE, INACTIVE = 'pending', 'active', 'inactive'


@dataclass(kw_only=True)
class ProviderEtf:
    """
    An actively managed equity ETF found from the SEC's N-CEN filings and FMP
    (modules/sec/etf_profile.py), with the FMP profile it was classified by. status is set by the
    selection rules (modules/sec/etf_selection.py): 'pending' until its first check (or its first
    holdings download), then 'active' - its holdings are downloaded on the daily run days
    (modules/cron/etf_downloader.py) and it feeds best ideas - or 'inactive'.
    Fields in table-column order (keyword-only, so the required ones needn't come first).
    """
    id: int | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    fmp_updated_at: datetime | None = None
    provider_id: int
    sec_series_id: str | None = None
    status: str = PENDING
    region: str | None = None                 # US / International / Global
    name: str | None = None
    description: str | None = None
    isin: str | None = None
    ticker: str | None = None
    cap_type: str | None = None               # large / mid / small / smid / all
    style_type: str | None = None             # value / growth / blend (large-cap funds only)
    trading_since: datetime | None = None
    website: str | None = None
    last_downloaded: datetime | None = None
    benchmark_id: int | None = None
    asset_class: str | None = None
    strategy: str | None = None
    aum: float | None = None
    nav: float | None = None
    nav_currency: str | None = None
    expense_ratio: float | None = None
    holdings_lines: int | None = None
    stock_holdings: int | None = None
    equity_weight: float | None = None
    stock_weight: float | None = None
    top10_weight: float | None = None
    us_weight: float | None = None
    avg_float_cap: float | None = None
    large_weight: float | None = None
    mid_weight: float | None = None
    small_weight: float | None = None
    value_weight: float | None = None
    growth_weight: float | None = None
    style_coverage: float | None = None
    sector_weights: dict | None = None
    top_sector: str | None = None
    top_sector_weight: float | None = None
    country_weights: dict | None = None       # ISO country -> share (a US-region company counts as US)
    top_country: str | None = None
    top_country_weight: float | None = None
    emerging_weight: float | None = None      # share in stocks the emerging index fund (VWO) holds


# Written by each profile; status, last_downloaded and benchmark_id are not.
PROFILE_COLUMNS = (
    'provider_id', 'sec_series_id', 'region', 'name', 'description', 'isin', 'ticker', 'cap_type', 'style_type',
    'trading_since', 'website', 'asset_class', 'strategy', 'aum', 'nav', 'nav_currency', 'expense_ratio',
    'holdings_lines', 'stock_holdings', 'equity_weight', 'stock_weight', 'top10_weight', 'us_weight', 'avg_float_cap',
    'large_weight', 'mid_weight', 'small_weight', 'value_weight', 'growth_weight', 'style_coverage',
    'sector_weights', 'top_sector', 'top_sector_weight', 'country_weights', 'top_country', 'top_country_weight',
    'emerging_weight',
)
_JSONB_COLUMNS = ('sector_weights', 'country_weights')

_UPSERT_SQL = sql.SQL(
    "INSERT INTO provider_etf ({columns}, fmp_updated_at) VALUES ({placeholders}, (now() AT TIME ZONE 'utc')) "
    "ON CONFLICT (sec_series_id) DO UPDATE SET {updates}, "
    "fmp_updated_at = (now() AT TIME ZONE 'utc'), updated_at = (now() AT TIME ZONE 'utc') "
    "RETURNING id, (xmax = 0) AS inserted;"
).format(
    columns=sql.SQL(", ").join(map(sql.Identifier, PROFILE_COLUMNS)),
    placeholders=sql.SQL(", ").join(sql.Placeholder() for _ in PROFILE_COLUMNS),
    updates=sql.SQL(", ").join(
        sql.SQL("{c} = EXCLUDED.{c}").format(c=sql.Identifier(c)) for c in PROFILE_COLUMNS if c != 'sec_series_id'
    ),
)


def upsert_profile(item: ProviderEtf) -> tuple[int, bool]:
    """Inserts the fund (pending) or updates its profile, by SEC series id. Returns (id, inserted)."""
    values = [Jsonb(getattr(item, c)) if c in _JSONB_COLUMNS and getattr(item, c) is not None else getattr(item, c)
              for c in PROFILE_COLUMNS]
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(_UPSERT_SQL, values)
                row = cur.fetchone()
            conn.commit()
        if row is None:
            raise Exception("no row returned")
        return int(row[0]), bool(row[1])
    except Error as e:
        raise Exception(f"Error saving provider ETF {item.ticker} ({item.sec_series_id}): {e}")


def fetch_all() -> list[ProviderEtf]:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(ProviderEtf)) as cur:
                cur.execute('SELECT * FROM provider_etf ORDER BY name;')
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error fetching the provider ETFs: {e}")


def fetch_active() -> list[ProviderEtf]:
    """The ETFs best ideas are generated for."""
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(ProviderEtf)) as cur:
                cur.execute('SELECT * FROM provider_etf WHERE status = %s ORDER BY provider_id, name;', (ACTIVE,))
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error fetching the active provider ETFs: {e}")


def fetch_by_id(id: int) -> ProviderEtf:
    with db_pool_instance.get_connection() as conn:
        with conn.cursor(row_factory=class_row(ProviderEtf)) as cur:
            cur.execute('SELECT * FROM provider_etf WHERE id = %s;', (id,))
            item = cur.fetchone()
    if item is None:
        raise Exception(f"Provider ETF not found for id={id}")
    return item


def set_strategy_by_series(series_id: str, strategy: str | None) -> None:
    """Records a fund's new strategy when its profile no longer finds it an equity fund - the
    selection then makes it inactive (the rest of its profile is kept as it was)."""
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE provider_etf SET strategy = %s, fmp_updated_at = (now() AT TIME ZONE 'utc'), "
                    "updated_at = (now() AT TIME ZONE 'utc') WHERE sec_series_id = %s AND strategy IS DISTINCT FROM %s;",
                    (strategy, series_id, strategy),
                )
            conn.commit()
    except Error as e:
        raise Exception(f"Error saving the strategy of provider ETF {series_id}: {e}")


def update_selection(changes: list[tuple[int, str, int | None]]) -> None:
    """Writes the selection's (id, status, benchmark_id) for the ETFs whose values changed."""
    if not changes:
        return
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    "UPDATE provider_etf SET status = %s, benchmark_id = %s, updated_at = (now() AT TIME ZONE 'utc') WHERE id = %s;",
                    [(status, benchmark_id, id) for id, status, benchmark_id in changes],
                )
            conn.commit()
    except Error as e:
        raise Exception(f"Error saving the provider ETF selection: {e}")


def set_last_downloaded(id: int, when: datetime) -> None:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('UPDATE provider_etf SET last_downloaded = %s WHERE id = %s;', (when, id))
            conn.commit()
    except Error as e:
        raise Exception(f"Error saving the last download of provider ETF {id}: {e}")
