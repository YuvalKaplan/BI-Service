import json
from datetime import datetime
from typing import Optional, Sequence
from psycopg.errors import Error
from psycopg.rows import class_row, dict_row
from dataclasses import dataclass
from modules.core.db import db_pool_instance

US_EXCHANGES = {'NYSE', 'NASDAQ'}


def is_preferred_exchange(candidate_exchange: str | None, current_exchange: str | None) -> bool:
    """
    True if candidate_exchange should replace current_exchange as the canonical listing
    for a symbol that has rows on multiple exchanges: NYSE/NASDAQ always wins over a
    non-US exchange; if neither (or both) are US-listed, the candidate — assumed to be
    the more recently seen one — wins.
    """
    if current_exchange is None:
        return True
    if candidate_exchange in US_EXCHANGES:
        return True
    return current_exchange not in US_EXCHANGES


@dataclass
class Ticker:
    symbol: str
    id: int = 0  # 0 until inserted; rows read from the DB always carry their id
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    source: str | None = None
    style_type: str | None = None
    cap_type: str | None = None
    type_from: str | None = None
    style_factors_failed_at: datetime | None = None
    isin: str | None = None
    cusip: str | None = None
    cik: str | None = None
    exchange: str | None = None
    name: str | None = None
    industry: str | None = None
    sector: str | None = None
    country: str | None = None
    currency: str | None = None
    esg_factors: dict | None = None
    esg_qualified: bool | None = None
    is_actively_trading: bool | None = None
    invalid: str | None = None
    master_ticker_id: int | None = None
    company_market_cap: float | None = None
    average_turnover: float | None = None  # FMP profile averageVolume x price, quote currency (major unit)
    verified_shares: float | None = None   # share count (FMP units) FMP's history is repaired to — see refresh.verify_share_count
    free_float: float | None = None        # FMP freeFloat % of the listing; 0 = no equity float (a note / preferred) — see free_float.refresh
    float_factor: float | None = None      # index funds' float cap / our company cap (masters): normally 0-1, > 1 = our cap looks too low; a cap check and share-count witness, not weights — see free_float.refresh
    region: str | None = None


# ── DB read ──────────────────────────────────────────────────────────────────

def fetch_by_symbol(symbol: str) -> Ticker | None:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Ticker)) as cur:
                cur.execute('SELECT * FROM ticker WHERE symbol = %s;', (symbol,))
                return cur.fetchone()
    except Error as e:
        raise Exception(f"Error fetching the Ticker from the DB: {e}")

def fetch_all_for_symbol_cache() -> dict[str, tuple[int, str] | None]:
    """
    Return {symbol: (ticker_id, exchange)} for valid tickers, {symbol: None} for symbols
    whose only rows are invalid. When a symbol has multiple rows (cross-listed on several
    exchanges), the NYSE/NASDAQ-listed row is kept as canonical; if none of the duplicates
    is US-listed, the most recently created row wins.
    """
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT symbol, id, invalid, exchange, created_at FROM ticker ORDER BY created_at;')
                rows = cur.fetchall()
    except Error as e:
        raise Exception(f"Error fetching all tickers for cache: {e}")

    cache: dict[str, tuple[int, str] | None] = {}
    for symbol, ticker_id, invalid, exchange, _created_at in rows:
        if invalid:
            cache.setdefault(symbol, None)
            continue
        current = cache.get(symbol)
        current_exchange = current[1] if current else None
        if is_preferred_exchange(exchange, current_exchange):
            cache[symbol] = (ticker_id, exchange or '')
    return cache

def fetch_all_for_isin_cache() -> dict[str, int | None]:
    """Return {isin: id} for valid tickers and {isin: None} for invalid ones."""
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT isin, id, invalid FROM ticker WHERE isin IS NOT NULL;')
                return {row[0]: None if row[2] else row[1] for row in cur.fetchall()}
    except Error as e:
        raise Exception(f"Error fetching all ISINs for cache: {e}")

def fetch_by_symbols(symbols: list[str]) -> list[Ticker]:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Ticker)) as cur:
                cur.execute("SELECT * FROM ticker WHERE symbol = ANY(%s);", (symbols,))
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error fetching the Ticker list from the DB: {e}")

def fetch_all_valid() -> list['Ticker']:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Ticker)) as cur:
                cur.execute("SELECT * FROM ticker WHERE invalid IS NULL;")
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error fetching all valid tickers: {e}")


def fetch_all_valid_companies() -> list['Ticker']:
    """Valid tickers that aren't a share-class sibling (master_ticker_id IS NULL) — one row per
    company, for company-level analysis (ESG) that doesn't need repeating per share class."""
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Ticker)) as cur:
                cur.execute("SELECT * FROM ticker WHERE invalid IS NULL AND master_ticker_id IS NULL;")
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error fetching all valid companies: {e}")


def fetch_companies_missing_esg() -> list['Ticker']:
    """Valid companies (masters/standalone) whose ESG has never been fetched (esg_factors IS NULL).
    A fetch that found no FMP data still stores all-None factors, so those aren't retried here."""
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Ticker)) as cur:
                cur.execute("""
                    SELECT * FROM ticker
                    WHERE invalid IS NULL AND master_ticker_id IS NULL AND esg_factors IS NULL;
                """)
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error fetching companies missing ESG: {e}")


def fetch_style_esg_stats() -> dict[str, int]:
    """Coverage counts over valid companies (masters/standalone), for data-fill reports."""
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute("""
                    SELECT count(*)                                                           AS companies,
                           count(*) FILTER (WHERE style_type IS NULL)                          AS style_missing,
                           count(*) FILTER (WHERE style_type IS NULL
                                              AND style_factors_failed_at IS NOT NULL)         AS style_factors_failed,
                           count(*) FILTER (WHERE type_from = 'CAT_ETF')                       AS style_cat_etf,
                           count(*) FILTER (WHERE type_from = 'PROVIDER_ETF')                  AS style_provider_etf,
                           count(*) FILTER (WHERE type_from = 'MODEL')                         AS style_model,
                           count(*) FILTER (WHERE esg_factors IS NULL)                         AS esg_missing,
                           count(*) FILTER (WHERE esg_factors IS NOT NULL
                                              AND esg_factors->>'esg_risk_rating' IS NULL
                                              AND esg_factors->>'esg_score' IS NULL
                                              AND esg_factors->>'governance_score' IS NULL)    AS esg_no_data,
                           count(*) FILTER (WHERE esg_qualified)                               AS esg_qualified
                    FROM ticker
                    WHERE invalid IS NULL AND master_ticker_id IS NULL;
                """)
                row = cur.fetchone()
                return dict(row) if row else {}
    except Error as e:
        raise Exception(f"Error fetching style/ESG coverage stats: {e}")


def clear_style_factors_failed_at() -> int:
    """Clear the 30-day retry back-off on unclassified companies so the classifier retries them now."""
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE ticker SET style_factors_failed_at = NULL
                    WHERE style_type IS NULL AND invalid IS NULL AND master_ticker_id IS NULL
                      AND style_factors_failed_at IS NOT NULL;
                """)
                return cur.rowcount
    except Error as e:
        raise Exception(f"Error clearing style_factors_failed_at: {e}")


def fetch_stale_tickers(include_invalid: bool = False) -> list['Ticker']:
    """
    Every ticker whose profile hasn't been refreshed via the FMP profile API within the last
    week (or ever) — the single shared candidate pool for any ticker-profile-refresh flow
    (scripts/data_fill_ticker_profile.py, scripts/sim_prep_data.py, the live cron's daily run
    step), so they all go over the same list instead of each having their own variant query.
    Excludes tickers already marked invalid unless include_invalid=True (retry them too).
    """
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Ticker)) as cur:
                if include_invalid:
                    cur.execute("""
                        SELECT * FROM ticker
                        WHERE updated_at IS NULL OR updated_at < NOW() - INTERVAL '7 days';
                    """)
                else:
                    cur.execute("""
                        SELECT * FROM ticker
                        WHERE invalid IS NULL
                          AND (updated_at IS NULL OR updated_at < NOW() - INTERVAL '7 days');
                    """)
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error fetching stale tickers: {e}")


def fetch_by_ids(ids: list[int]) -> list[Ticker]:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Ticker)) as cur:
                cur.execute("SELECT * FROM ticker WHERE id = ANY(%s);", (ids,))
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error fetching Tickers by ids from the DB: {e}")

def fetch_by_isin_and_symbol(isin: str, symbol: str, exchange: str | None = None) -> Ticker | None:
    """Fetch by ISIN + base symbol (suffix stripped) + optional exchange.
    Handles cross-listed stocks that share an ISIN across exchanges."""
    import re as _re
    base_symbol = _re.split(r'[\s.]', symbol)[0]
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Ticker)) as cur:
                if exchange:
                    cur.execute(
                        'SELECT * FROM ticker WHERE isin = %s AND symbol = %s AND exchange = %s;',
                        (isin, base_symbol, exchange),
                    )
                else:
                    cur.execute(
                        'SELECT * FROM ticker WHERE isin = %s AND symbol = %s;',
                        (isin, base_symbol),
                    )
                return cur.fetchone()
    except Error as e:
        raise Exception(f"Error fetching Ticker by ISIN and symbol from the DB: {e}")


# ── DB write ─────────────────────────────────────────────────────────────────

def upsert_by_symbol(item: Ticker) -> tuple[int, bool]:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                query = """
                    INSERT INTO ticker (symbol, isin, cusip, cik, name, exchange, industry, sector, country, currency, source, type_from, is_actively_trading, average_turnover)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol, exchange)
                    DO UPDATE
                    SET isin = COALESCE(EXCLUDED.isin, ticker.isin),
                        cusip = COALESCE(EXCLUDED.cusip, ticker.cusip),
                        cik = COALESCE(EXCLUDED.cik, ticker.cik),
                        name = EXCLUDED.name,
                        exchange = EXCLUDED.exchange,
                        industry = COALESCE(EXCLUDED.industry, ticker.industry),
                        sector = COALESCE(EXCLUDED.sector, ticker.sector),
                        country = COALESCE(EXCLUDED.country, ticker.country),
                        currency = COALESCE(EXCLUDED.currency, ticker.currency),
                        source = EXCLUDED.source,
                        type_from = COALESCE(EXCLUDED.type_from, ticker.type_from),
                        is_actively_trading = EXCLUDED.is_actively_trading,
                        average_turnover = COALESCE(EXCLUDED.average_turnover, ticker.average_turnover)
                    RETURNING id, (xmax = 0) AS is_new;
                """
                cur.execute(query, (item.symbol, item.isin, item.cusip, item.cik, item.name, item.exchange, item.industry, item.sector, item.country, item.currency, item.source, item.type_from, item.is_actively_trading, item.average_turnover))
                row = cur.fetchone()
                if row is None:
                    raise Exception("INSERT ... RETURNING id returned no row")
                return row[0], row[1]
    except Error as e:
        raise Exception(f"Error upserting ticker by symbol into the DB: {e}")

def update(item: Ticker) -> None:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE ticker
                    SET isin                 = %s,
                        cusip                = %s,
                        cik                  = %s,
                        name                 = %s,
                        exchange             = %s,
                        industry             = %s,
                        sector               = %s,
                        country              = %s,
                        currency             = %s,
                        source               = %s,
                        type_from            = %s,
                        is_actively_trading  = %s,
                        average_turnover     = COALESCE(%s, average_turnover),
                        updated_at           = NOW()
                    WHERE id = %s;
                """, (item.isin, item.cusip, item.cik, item.name, item.exchange,
                      item.industry, item.sector, item.country, item.currency, item.source, item.type_from,
                      item.is_actively_trading, item.average_turnover,
                      item.id))
    except Error as e:
        raise Exception(f"Error updating ticker {item.id}: {e}")


def update_esg_qualified(symbols: list[str]) -> None:
    if not symbols:
        return
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE ticker
                    SET esg_qualified = TRUE
                    WHERE symbol = ANY(%s::text[]);
                """, (symbols,))
    except Error as e:
        raise Exception(f"Error updating esg_qualified in the DB: {e}")

def update_esg_data(ticker_id: int, esg_qualified: bool, esg_factors: dict) -> None:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE ticker
                    SET esg_qualified = %s,
                        esg_factors   = %s
                    WHERE id = %s;
                """, (esg_qualified, json.dumps(esg_factors), ticker_id))
    except Error as e:
        raise Exception(f"Error updating esg data for ticker {ticker_id}: {e}")

def fetch_with_missing_exchange() -> list['Ticker']:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Ticker)) as cur:
                cur.execute("SELECT * FROM ticker WHERE exchange IS NULL AND invalid IS NULL;")
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error fetching tickers with missing exchange: {e}")


def fetch_with_missing_country() -> list['Ticker']:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Ticker)) as cur:
                cur.execute("SELECT * FROM ticker WHERE country IS NULL AND exchange IS NOT NULL AND invalid IS NULL;")
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error fetching tickers with missing country: {e}")


def update_invalid(ticker_id: int, reason: str | None) -> None:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE ticker SET invalid = %s WHERE id = %s;", (reason, ticker_id))
    except Error as e:
        raise Exception(f"Error updating the Ticker invalid reason into the DB: {e}")


def clear_invalid_with_prefix(prefix: str) -> int:
    """Clears the invalid flag of every ticker whose reason starts with `prefix`. Returns how many."""
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE ticker SET invalid = NULL WHERE invalid LIKE %s;", (prefix.replace('%', r'\%') + '%',))
                return cur.rowcount
    except Error as e:
        raise Exception(f"Error clearing the invalid flags starting with '{prefix}': {e}")


def update_style_from_categorization_etfs() -> None:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE public.ticker t
                    SET style_type = ct.style_type,
                        cap_type   = ct.cap_type,
                        type_from  = 'CAT_ETF'
                    FROM public.categorize_ticker ct
                    WHERE t.symbol        = ct.symbol
                      AND ct.style_type IS NOT NULL
                      AND t.invalid     IS NULL
                      AND t.master_ticker_id IS NULL;
                """)
    except Error as e:
        raise Exception(f"Error updating ticker style from categorization ETFs: {e}")

def update_style_for_unclassified() -> None:
    """Set style/cap for tickers that have never been classified, using existing categorize ETF data."""
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE public.ticker t
                    SET style_type = ct.style_type,
                        cap_type   = ct.cap_type,
                        type_from  = 'CAT_ETF'
                    FROM public.categorize_ticker ct
                    WHERE t.symbol        = ct.symbol
                      AND t.exchange      = ct.exchange
                      AND ct.style_type   IS NOT NULL
                      AND t.style_type    IS NULL
                      AND t.invalid       IS NULL
                      AND t.master_ticker_id IS NULL;
                """)
    except Error as e:
        raise Exception(f"Error updating style for unclassified tickers: {e}")


def update_style_from_provider_etfs() -> None:
    """Fill style_type for unclassified tickers using provider_etf holdings (value/growth only)."""
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE public.ticker t
                    SET style_type = pe.style_type,
                        type_from  = 'PROVIDER_ETF'
                    FROM public.provider_etf_holding peh
                    JOIN public.provider_etf pe ON pe.id = peh.provider_etf_id
                    WHERE t.id         = peh.ticker_id
                      AND t.style_type IS NULL
                      AND t.invalid    IS NULL
                      AND t.master_ticker_id IS NULL
                      AND pe.style_type IN ('value', 'growth');
                """)
    except Error as e:
        raise Exception(f"Error updating ticker style from provider ETFs: {e}")


def fetch_new_tickers_for_style() -> list['Ticker']:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Ticker)) as cur:
                cur.execute("""
                    SELECT id, symbol FROM ticker
                    WHERE style_type IS NULL AND invalid IS NULL
                      AND master_ticker_id IS NULL
                      AND style_factors_failed_at IS NULL
                """)
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error fetching new tickers for style classification: {e}")


def fetch_retry_tickers_for_style() -> list['Ticker']:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Ticker)) as cur:
                cur.execute("""
                    SELECT id, symbol FROM ticker
                    WHERE style_type IS NULL AND invalid IS NULL
                      AND master_ticker_id IS NULL
                      AND style_factors_failed_at < NOW() - INTERVAL '30 days'
                    LIMIT 200
                """)
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error fetching retry tickers for style classification: {e}")


def update_style_from_model_bulk(updates: list[dict]) -> None:
    if not updates:
        return
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    "UPDATE ticker SET style_type = %s, type_from = 'MODEL' WHERE id = %s",
                    [(u["style_type"], u["ticker_id"]) for u in updates]
                )
    except Error as e:
        raise Exception(f"Error bulk-updating ticker style from model: {e}")


def update_style_factors_failed_at_bulk(ticker_ids: list[int]) -> None:
    if not ticker_ids:
        return
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    "UPDATE ticker SET style_factors_failed_at = NOW() WHERE id = %s",
                    [(tid,) for tid in ticker_ids]
                )
    except Error as e:
        raise Exception(f"Error bulk-updating style_factors_failed_at: {e}")


# ── Multi-ticker (share-class) company consolidation ────────────────────────

def fetch_cik_groups() -> list[tuple[str, list[int]]]:
    """Returns (cik, member_ids) for every CIK shared by 2+ valid tickers."""
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT cik, array_agg(id ORDER BY id)
                    FROM ticker
                    WHERE cik IS NOT NULL AND cik <> '' AND invalid IS NULL
                    GROUP BY cik
                    HAVING COUNT(*) > 1;
                """)
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error fetching cik groups: {e}")


def fetch_no_cik_tickers() -> list['Ticker']:
    """Tickers with no cik (whether or not a master is already assigned — an existing
    master must still be visible here so name-based grouping can reuse it),
    candidates for the name-matching fallback pass."""
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Ticker)) as cur:
                cur.execute("""
                    SELECT * FROM ticker
                    WHERE (cik IS NULL OR cik = '')
                      AND invalid IS NULL
                      AND name IS NOT NULL AND name <> '';
                """)
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error fetching no-cik tickers: {e}")


def update_master_ticker_bulk(pairs: Sequence[tuple[int, int | None]]) -> None:
    """pairs: [(ticker_id, master_ticker_id), ...]; a None master_ticker_id clears the link."""
    if not pairs:
        return
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    "UPDATE ticker SET master_ticker_id = %s WHERE id = %s",
                    [(master_id, tid) for tid, master_id in pairs]
                )
    except Error as e:
        raise Exception(f"Error bulk-updating master_ticker_id: {e}")


def fetch_linked_siblings() -> list['Ticker']:
    """Every ticker currently pointing at a master (master_ticker_id set)."""
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Ticker)) as cur:
                cur.execute("SELECT * FROM ticker WHERE master_ticker_id IS NOT NULL;")
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error fetching linked sibling tickers: {e}")


def clear_master_ticker_bulk(ticker_ids: list[int]) -> None:
    if not ticker_ids:
        return
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE ticker SET master_ticker_id = NULL WHERE id = ANY(%s);", (ticker_ids,))
    except Error as e:
        raise Exception(f"Error clearing master_ticker_id: {e}")


def clear_derived_data_bulk(ticker_ids: list[int]) -> None:
    """Clears what was derived from a listing's FMP data - style, ESG, free float and float
    factor, turnover, verified share count, company cap and region - and its invalid flag, for a
    listing whose data turned out to be another company's (scripts/data_fix_symbol_encoding.py);
    the ticker maintenance fills them again."""
    if not ticker_ids:
        return
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE ticker
                    SET style_type = NULL, cap_type = NULL, type_from = NULL, style_factors_failed_at = NULL,
                        esg_factors = NULL, esg_qualified = NULL, free_float = NULL, float_factor = NULL,
                        average_turnover = NULL, verified_shares = NULL, company_market_cap = NULL,
                        region = NULL, invalid = NULL
                    WHERE id = ANY(%s);
                """, (ticker_ids,))
    except Error as e:
        raise Exception(f"Error clearing derived ticker data: {e}")


def clear_stale_company_market_caps() -> int:
    """NULLs company_market_cap on any ticker that's no longer a master (no sibling points at
    it) — otherwise a former master keeps its old company cap after being unlinked."""
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE ticker t
                    SET company_market_cap = NULL
                    WHERE t.company_market_cap IS NOT NULL
                      AND NOT EXISTS (SELECT 1 FROM ticker s WHERE s.master_ticker_id = t.id);
                """)
                return cur.rowcount
    except Error as e:
        raise Exception(f"Error clearing stale company_market_cap: {e}")


def fetch_master_groups() -> list[tuple[int, list[int]]]:
    """Returns (master_ticker_id, sibling_ids) for every currently-established master."""
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT master_ticker_id, array_agg(id)
                    FROM ticker
                    WHERE master_ticker_id IS NOT NULL
                    GROUP BY master_ticker_id;
                """)
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error fetching master groups: {e}")


def update_company_market_cap_bulk(pairs: list[tuple[int, float | None]]) -> None:
    """pairs: [(master_ticker_id, company_market_cap), ...]"""
    if not pairs:
        return
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    "UPDATE ticker SET company_market_cap = %s WHERE id = %s",
                    [(cap, tid) for tid, cap in pairs]
                )
    except Error as e:
        raise Exception(f"Error bulk-updating company_market_cap: {e}")


def update_average_turnover_bulk(pairs: list[tuple[int, float]]) -> None:
    """pairs: [(ticker_id, average_turnover), ...]"""
    if not pairs:
        return
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    "UPDATE ticker SET average_turnover = %s WHERE id = %s",
                    [(v, tid) for tid, v in pairs]
                )
    except Error as e:
        raise Exception(f"Error bulk-updating average_turnover: {e}")


def update_free_float_bulk(pairs: list[tuple[int, float | None]]) -> None:
    """pairs: [(ticker_id, free_float %), ...] — only rows whose value actually changes are written."""
    if not pairs:
        return
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    "UPDATE ticker SET free_float = %s WHERE id = %s AND free_float IS DISTINCT FROM %s",
                    [(v, tid, v) for tid, v in pairs]
                )
    except Error as e:
        raise Exception(f"Error bulk-updating free_float: {e}")


def update_float_factor_bulk(pairs: list[tuple[int, float | None]]) -> None:
    """pairs: [(company ticker_id, float_factor), ...] — only rows whose value actually changes are written."""
    if not pairs:
        return
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    "UPDATE ticker SET float_factor = %s WHERE id = %s AND float_factor IS DISTINCT FROM %s",
                    [(v, tid, v) for tid, v in pairs]
                )
    except Error as e:
        raise Exception(f"Error bulk-updating float_factor: {e}")


def update_verified_shares(ticker_id: int, shares: float | None) -> None:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE ticker SET verified_shares = %s WHERE id = %s", (shares, ticker_id))
    except Error as e:
        raise Exception(f"Error updating verified_shares for ticker {ticker_id}: {e}")


def update_region_bulk(pairs: list[tuple[int, str]]) -> None:
    """pairs: [(ticker_id, region), ...] — only rows whose region actually changes are written."""
    if not pairs:
        return
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    "UPDATE ticker SET region = %s WHERE id = %s AND region IS DISTINCT FROM %s",
                    [(r, tid, r) for tid, r in pairs]
                )
    except Error as e:
        raise Exception(f"Error bulk-updating region: {e}")


def fetch_all() -> list['Ticker']:
    """Every ticker row, valid or not — for company-level derivations over all listings."""
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Ticker)) as cur:
                cur.execute("SELECT * FROM ticker;")
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error fetching all tickers: {e}")


def fetch_cik_tickers() -> list['Ticker']:
    """Valid tickers carrying a CIK — the name index that no-CIK listings are matched against."""
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Ticker)) as cur:
                cur.execute("SELECT * FROM ticker WHERE cik IS NOT NULL AND cik <> '' AND invalid IS NULL;")
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error fetching CIK tickers: {e}")


def fetch_group_members(master_ids: list[int]) -> dict[int, list['Ticker']]:
    """{master_id: [the master, then each sibling]} for the given masters."""
    if not master_ids:
        return {}
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Ticker)) as cur:
                cur.execute("SELECT * FROM ticker WHERE id = ANY(%s) OR master_ticker_id = ANY(%s);", (master_ids, master_ids))
                rows = cur.fetchall()
    except Error as e:
        raise Exception(f"Error fetching group members: {e}")
    groups: dict[int, list[Ticker]] = {mid: [] for mid in master_ids}
    for t in sorted(rows, key=lambda t: (t.master_ticker_id is not None, t.id)):
        root = t.master_ticker_id if t.master_ticker_id in groups else t.id
        if root in groups:
            groups[root].append(t)
    return groups


def fetch_master_info_by_ids(ticker_ids: list[int]) -> dict[int, tuple[int | None, float | None]]:
    """Returns {ticker_id: (master_ticker_id, company_market_cap)}."""
    if not ticker_ids:
        return {}
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, master_ticker_id, company_market_cap FROM ticker WHERE id = ANY(%s);",
                    (ticker_ids,)
                )
                return {row[0]: (row[1], row[2]) for row in cur.fetchall()}
    except Error as e:
        raise Exception(f"Error fetching master info by ids: {e}")
