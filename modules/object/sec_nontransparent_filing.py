from datetime import date, datetime
from dataclasses import dataclass
from psycopg.errors import Error
from modules.core.db import db_pool_instance


@dataclass
class SecNontransparentFiling:
    """
    A single-fund prospectus filing (497K / 497) whose text carries the non-transparent ETF
    legend, for the series it covers (modules/sec/prospectus.py) - the evidence behind the
    selection's 'transparent' rule.
    """
    accession_number: str
    series_id: str
    cik: str
    form_type: str
    filing_date: date
    created_at: datetime | None = None
    updated_at: datetime | None = None


def fetch_accessions() -> set[str]:
    """Accession numbers of the stored filings."""
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT DISTINCT accession_number FROM sec_nontransparent_filing;')
                return {row[0] for row in cur.fetchall()}
    except Error as e:
        raise Exception(f"Error fetching the non-transparent prospectus filings: {e}")


def fetch_series(since: date) -> set[str]:
    """The series with a stored filing on or after since."""
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT DISTINCT series_id FROM sec_nontransparent_filing WHERE filing_date >= %s;', (since,))
                return {row[0] for row in cur.fetchall()}
    except Error as e:
        raise Exception(f"Error fetching the non-transparent series: {e}")


def upsert_many(items: list[SecNontransparentFiling]) -> None:
    if not items:
        return
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO sec_nontransparent_filing (accession_number, series_id, cik, form_type, filing_date) "
                    "VALUES (%s, %s, %s, %s, %s) "
                    "ON CONFLICT (accession_number, series_id) DO UPDATE SET cik = EXCLUDED.cik, "
                    "form_type = EXCLUDED.form_type, filing_date = EXCLUDED.filing_date, "
                    "updated_at = (now() AT TIME ZONE 'utc');",
                    [(i.accession_number, i.series_id, i.cik, i.form_type, i.filing_date) for i in items])
            conn.commit()
    except Error as e:
        raise Exception(f"Error saving non-transparent prospectus filings: {e}")
