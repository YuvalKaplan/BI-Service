from datetime import date, datetime
from dataclasses import dataclass
from psycopg import sql
from psycopg.errors import Error
from modules.core.db import db_pool_instance


@dataclass
class SecNcenFiling:
    """
    One Form N-CEN / N-CEN/A filing read from EDGAR by modules/sec/ncen.py, with what it held.
    A filing stored with an error (download or parse failure) is read again on the next run.
    """
    accession_number: str
    cik: str
    registrant_name: str | None
    form_type: str
    filing_date: date
    report_period: date | None = None
    funds: int | None = None        # series in the filing
    etfs: int | None = None
    active_etfs: int | None = None
    error: str | None = None
    processed_at: datetime | None = None


_COLUMNS = ('accession_number', 'cik', 'registrant_name', 'form_type', 'filing_date', 'report_period',
            'funds', 'etfs', 'active_etfs', 'error')
_UPSERT_SQL = sql.SQL(
    "INSERT INTO sec_ncen_filing ({columns}) VALUES ({placeholders}) "
    "ON CONFLICT (accession_number) DO UPDATE SET {updates}, processed_at = (now() AT TIME ZONE 'utc');"
).format(
    columns=sql.SQL(", ").join(map(sql.Identifier, _COLUMNS)),
    placeholders=sql.SQL(", ").join(sql.Placeholder() for _ in _COLUMNS),
    updates=sql.SQL(", ").join(
        sql.SQL("{c} = EXCLUDED.{c}").format(c=sql.Identifier(c)) for c in _COLUMNS if c != 'accession_number'
    ),
)


def fetch_processed_accessions() -> set[str]:
    """Accession numbers of the filings read without an error."""
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT accession_number FROM sec_ncen_filing WHERE error IS NULL;')
                return {row[0] for row in cur.fetchall()}
    except Error as e:
        raise Exception(f"Error fetching the processed N-CEN filings: {e}")


def upsert(item: SecNcenFiling) -> None:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(_UPSERT_SQL, tuple(getattr(item, c) for c in _COLUMNS))
            conn.commit()
    except Error as e:
        raise Exception(f"Error saving N-CEN filing {item.accession_number}: {e}")
