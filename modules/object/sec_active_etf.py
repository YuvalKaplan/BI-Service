from datetime import date, datetime
from typing import List
from dataclasses import dataclass
from psycopg import sql
from psycopg.errors import Error
from psycopg.rows import class_row
from modules.core.db import db_pool_instance


@dataclass(kw_only=True)
class SecActiveEtf:
    """
    An actively managed ETF - an exchange-traded fund whose latest Form N-CEN doesn't mark it an
    index fund - as of that filing (one row per SEC series). Written by modules/sec/ncen.py.
    Fields in table-column order (keyword-only, so the required ones needn't come first).
    """
    series_id: str
    created_at: datetime | None = None
    updated_at: datetime | None = None
    fund_name: str
    ticker: str | None = None
    registrant_name: str | None = None       # the trust
    registrant_cik: str | None = None
    adviser_name: str | None = None          # investment adviser (sub-advisers left out)
    is_etmf: bool = False
    is_fund_of_funds: bool = False
    is_multiple_inverse: bool = False        # seeks a multiple / inverse of an index
    net_assets: float | None = None          # monthly average net assets, USD
    report_period: date | None = None
    filing_date: date
    accession_number: str
    terminated_at: date | None = None        # listed as terminated by a later filing of its registrant


_WRITE_COLUMNS = ('series_id', 'fund_name', 'ticker', 'registrant_name', 'registrant_cik', 'adviser_name',
                  'is_etmf', 'is_fund_of_funds', 'is_multiple_inverse', 'net_assets', 'report_period',
                  'filing_date', 'accession_number')

# A row only ever moves to a newer filing: (report period, filing date) decides, so a late
# amendment for an older period or a filing read again never overwrites a newer one. A filing
# dated after the fund's termination month shows it's still running.
_UPSERT_SQL = sql.SQL(
    "INSERT INTO sec_active_etf ({columns}) VALUES ({placeholders}) "
    "ON CONFLICT (series_id) DO UPDATE SET {updates}, "
    "updated_at = (now() AT TIME ZONE 'utc'), "
    "terminated_at = CASE WHEN EXCLUDED.filing_date > sec_active_etf.terminated_at THEN NULL ELSE sec_active_etf.terminated_at END "
    "WHERE (COALESCE(EXCLUDED.report_period, '-infinity'::date), EXCLUDED.filing_date) "
    ">= (COALESCE(sec_active_etf.report_period, '-infinity'::date), sec_active_etf.filing_date) "
    "RETURNING (xmax = 0) AS inserted;"
).format(
    columns=sql.SQL(", ").join(map(sql.Identifier, _WRITE_COLUMNS)),
    placeholders=sql.SQL(", ").join(sql.Placeholder() for _ in _WRITE_COLUMNS),
    updates=sql.SQL(", ").join(
        sql.SQL("{c} = EXCLUDED.{c}").format(c=sql.Identifier(c)) for c in _WRITE_COLUMNS if c != 'series_id'
    ),
)

_REMOVE_SQL = (
    "DELETE FROM sec_active_etf WHERE series_id = ANY(%s) "
    "AND (COALESCE(report_period, '-infinity'::date), filing_date) <= (COALESCE(%s::date, '-infinity'::date), %s::date);"
)

_TERMINATE_SQL = (
    "UPDATE sec_active_etf SET terminated_at = %s, updated_at = (now() AT TIME ZONE 'utc') "
    "WHERE series_id = %s AND filing_date <= %s AND terminated_at IS DISTINCT FROM %s;"
)


def apply_filing(
    items: List[SecActiveEtf],
    removed_series: List[str],
    terminated: List[tuple[str, date]],
    report_period: date | None,
    filing_date: date,
) -> tuple[int, int, int, int]:
    """
    Applies one N-CEN filing, in one transaction: upserts its active ETFs, deletes the series it
    reports as index funds or not ETFs (where the stored row isn't from a newer filing), and marks
    its terminated series (series_id, termination date). Returns (added, updated, removed, terminated).
    """
    added = updated = removed = marked = 0
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                for item in items:
                    cur.execute(_UPSERT_SQL, tuple(getattr(item, c) for c in _WRITE_COLUMNS))
                    row = cur.fetchone()
                    if row is not None:
                        added += row[0]
                        updated += not row[0]
                if removed_series:
                    cur.execute(_REMOVE_SQL, (removed_series, report_period, filing_date))
                    removed = cur.rowcount
                for series_id, terminated_at in terminated:
                    cur.execute(_TERMINATE_SQL, (terminated_at, series_id, filing_date, terminated_at))
                    marked += cur.rowcount
            conn.commit()
        return added, updated, removed, marked
    except Error as e:
        raise Exception(f"Error applying the N-CEN filing of {filing_date} to sec_active_etf: {e}")


def fetch_current(filed_since: date) -> List[SecActiveEtf]:
    """The current list: not terminated, with a filing on or after filed_since."""
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(SecActiveEtf)) as cur:
                cur.execute(
                    'SELECT * FROM sec_active_etf WHERE terminated_at IS NULL AND filing_date >= %s ORDER BY fund_name;',
                    (filed_since,),
                )
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error fetching the current active ETFs: {e}")


def count_current(filed_since: date) -> tuple[int, int]:
    """(current active ETFs, how many of them are in provider_etf - profiled as equity funds)."""
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT count(*), count(*) FILTER (WHERE EXISTS (
                           SELECT 1 FROM provider_etf p WHERE p.sec_series_id = a.series_id))
                       FROM sec_active_etf a
                       WHERE a.terminated_at IS NULL AND a.filing_date >= %s;""",
                    (filed_since,),
                )
                row = cur.fetchone()
                return (row[0], row[1]) if row else (0, 0)
    except Error as e:
        raise Exception(f"Error counting the current active ETFs: {e}")


def fetch_old_tracked_tickers() -> dict[str, tuple[int, bool, str | None, str | None, str | None]]:
    """old_provider_etf ticker (upper case) -> (old id, enabled, cap_type, style_type, region): the
    ETFs chosen by hand before the selection rules, to compare the two."""
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT upper(e.ticker), e.id, NOT e.disabled AND NOT p.disabled, e.cap_type, e.style_type, e.region
                       FROM old_provider_etf e JOIN old_provider p ON p.id = e.provider_id
                       WHERE e.ticker IS NOT NULL;"""
                )
                return {row[0]: (row[1], row[2], row[3], row[4], row[5]) for row in cur.fetchall()}
    except Error as e:
        raise Exception(f"Error fetching the old provider ETF tickers: {e}")
