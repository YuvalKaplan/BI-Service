from datetime import datetime
from dataclasses import dataclass
from psycopg import sql
from psycopg.errors import Error
from psycopg.rows import class_row
from psycopg.types.json import Jsonb
from modules.core.db import db_pool_instance
from modules.object.test_provider import AWAITING_APPROVAL

NOT_EQUITY_REASON = 'No longer an actively managed equity fund by its profile'
SEC_GONE_REASON = 'No longer on the SEC active ETF list (terminated, index fund, or no recent filing)'


@dataclass(kw_only=True)
class TestProviderEtf:
    """
    An actively managed equity ETF found from the SEC's N-CEN filings and FMP
    (modules/sec/etf_profile.py) - parallel to provider_etf, without its scraping configuration,
    with the FMP profile it was classified by. region / cap_type / style_type / trading_since /
    website are provider_etf's own columns, filled from the profile. Disabled until an admin
    approves it; its holdings are stored once (test_provider_etf_holding).
    Fields in table-column order (keyword-only, so the required ones needn't come first).
    """
    id: int | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    fmp_updated_at: datetime | None = None
    test_provider_id: int
    sec_series_id: str | None = None
    disabled: bool = True
    disabled_reason: str | None = AWAITING_APPROVAL
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


# Written by each profile; disabled / disabled_reason, last_downloaded and benchmark_id are not.
PROFILE_COLUMNS = (
    'test_provider_id', 'sec_series_id', 'region', 'name', 'description', 'isin', 'ticker', 'cap_type', 'style_type',
    'trading_since', 'website', 'asset_class', 'strategy', 'aum', 'nav', 'nav_currency', 'expense_ratio',
    'holdings_lines', 'stock_holdings', 'equity_weight', 'stock_weight', 'top10_weight', 'us_weight', 'avg_float_cap',
    'large_weight', 'mid_weight', 'small_weight', 'value_weight', 'growth_weight', 'style_coverage',
    'sector_weights', 'top_sector', 'top_sector_weight',
)

# A fund disabled by the profile itself (it had stopped passing) is back to awaiting approval
# when it passes again; one an admin disabled keeps its reason.
_UPSERT_SQL = sql.SQL(
    "INSERT INTO test_provider_etf ({columns}, fmp_updated_at) VALUES ({placeholders}, (now() AT TIME ZONE 'utc')) "
    "ON CONFLICT (sec_series_id) DO UPDATE SET {updates}, "
    "fmp_updated_at = (now() AT TIME ZONE 'utc'), updated_at = (now() AT TIME ZONE 'utc'), "
    "disabled_reason = CASE WHEN test_provider_etf.disabled_reason IN ({not_equity}, {sec_gone}) "
    "THEN {awaiting} ELSE test_provider_etf.disabled_reason END "
    "RETURNING id, (xmax = 0) AS inserted;"
).format(
    columns=sql.SQL(", ").join(map(sql.Identifier, PROFILE_COLUMNS)),
    placeholders=sql.SQL(", ").join(sql.Placeholder() for _ in PROFILE_COLUMNS),
    updates=sql.SQL(", ").join(
        sql.SQL("{c} = EXCLUDED.{c}").format(c=sql.Identifier(c)) for c in PROFILE_COLUMNS if c != 'sec_series_id'
    ),
    not_equity=sql.Literal(NOT_EQUITY_REASON),
    sec_gone=sql.Literal(SEC_GONE_REASON),
    awaiting=sql.Literal(AWAITING_APPROVAL),
)


def upsert_profile(item: TestProviderEtf) -> tuple[int, bool]:
    """Inserts the fund (disabled, awaiting approval) or updates its profile, by SEC series id.
    Returns (id, inserted)."""
    values = [Jsonb(item.sector_weights) if c == 'sector_weights' and item.sector_weights is not None else getattr(item, c)
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
        raise Exception(f"Error saving test provider ETF {item.ticker} ({item.sec_series_id}): {e}")


def fetch_all() -> list[TestProviderEtf]:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(TestProviderEtf)) as cur:
                cur.execute('SELECT * FROM test_provider_etf ORDER BY name;')
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error fetching the test provider ETFs: {e}")


def disable_by_series(series_ids: list[str], reason: str) -> int:
    """Disables the funds of these SEC series that aren't disabled yet; returns how many."""
    if not series_ids:
        return 0
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE test_provider_etf SET disabled = true, disabled_reason = %s, updated_at = (now() AT TIME ZONE 'utc') "
                    "WHERE sec_series_id = ANY(%s) AND NOT disabled;",
                    (reason, series_ids),
                )
                count = cur.rowcount
            conn.commit()
        return count
    except Error as e:
        raise Exception(f"Error disabling test provider ETFs: {e}")


def set_last_downloaded(id: int, when: datetime) -> None:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('UPDATE test_provider_etf SET last_downloaded = %s WHERE id = %s;', (when, id))
            conn.commit()
    except Error as e:
        raise Exception(f"Error saving the last download of test provider ETF {id}: {e}")
