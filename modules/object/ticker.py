import json
from datetime import datetime
from psycopg.errors import Error
from psycopg.rows import class_row
from dataclasses import dataclass
from modules.core.db import db_pool_instance

@dataclass
class Ticker:
    id: int | None
    symbol: str
    created_at: datetime | None
    source: str | None
    style_type: str | None
    cap_type: str | None
    type_from: str | None
    isin: str | None
    cusip: str | None
    cik: str | None
    exchange: str | None
    name: str | None
    industry: str | None
    sector: str | None
    currency: str | None
    esg_factors: dict | None
    esg_qualified: bool | None
    invalid: str | None

    def __init__(self, symbol: str, created_at: datetime | None = None,
                 source: str | None = None, style_type: str | None = None, cap_type: str | None = None, type_from: str | None = None,
                 isin: str | None = None, cusip: str | None = None, cik: str | None = None, exchange: str | None = None,
                 name: str | None = None, industry: str | None = None, sector: str | None = None,
                 currency: str | None = None, esg_factors: dict | None = None,
                 esg_qualified: bool | None = None, invalid: str | None = None,
                 id: int | None = None):
        self.id = id
        self.symbol = symbol
        self.created_at = created_at
        self.source = source
        self.style_type = style_type
        self.cap_type = cap_type
        self.type_from = type_from
        self.isin = isin
        self.cusip = cusip
        self.cik = cik
        self.exchange = exchange
        self.name = name
        self.industry = industry
        self.sector = sector
        self.currency = currency
        self.esg_factors = esg_factors
        self.esg_qualified = esg_qualified
        self.invalid = invalid


# ── DB read ──────────────────────────────────────────────────────────────────

def fetch_by_symbol(symbol: str) -> Ticker | None:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Ticker)) as cur:
                cur.execute('SELECT * FROM ticker WHERE symbol = %s;', (symbol,))
                return cur.fetchone()
    except Error as e:
        raise Exception(f"Error fetching the Ticker from the DB: {e}")

def fetch_by_symbols(symbols: list[str]) -> list[Ticker]:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Ticker)) as cur:
                cur.execute("SELECT * FROM ticker WHERE symbol = ANY(%s);", (symbols,))
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error fetching the Ticker list from the DB: {e}")

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

def upsert_by_symbol(item: Ticker) -> int:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                query = """
                    INSERT INTO ticker (symbol, isin, cusip, cik, name, exchange, industry, sector, currency, source, type_from)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol)
                    DO UPDATE
                    SET isin = EXCLUDED.isin,
                        cusip = EXCLUDED.cusip,
                        cik = EXCLUDED.cik,
                        name = EXCLUDED.name,
                        exchange = EXCLUDED.exchange,
                        industry = EXCLUDED.industry,
                        sector = EXCLUDED.sector,
                        currency = EXCLUDED.currency,
                        source = EXCLUDED.source,
                        type_from = EXCLUDED.type_from
                    RETURNING id;
                """
                cur.execute(query, (item.symbol, item.isin, item.cusip, item.cik, item.name, item.exchange, item.industry, item.sector, item.currency, item.source, item.type_from))
                row = cur.fetchone()
                if row is None:
                    raise Exception("INSERT ... RETURNING id returned no row")
                return row[0]
    except Error as e:
        raise Exception(f"Error upserting ticker by symbol into the DB: {e}")

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

def update_esg_data(symbol: str, esg_qualified: bool, esg_factors: dict) -> None:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE ticker
                    SET esg_qualified = %s,
                        esg_factors   = %s
                    WHERE symbol = %s;
                """, (esg_qualified, json.dumps(esg_factors), symbol))
    except Error as e:
        raise Exception(f"Error updating esg data for {symbol}: {e}")

def update_invalid(symbol: str, reason: str) -> None:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE ticker SET invalid = %s WHERE symbol = %s;", (reason, symbol))
    except Error as e:
        raise Exception(f"Error updating the Ticker invalid reason into the DB: {e}")

def sanitize() -> None:
    """Mark any ticker table rows with non-standard symbols as invalid."""
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT sanitize_tickers();')
    except Error as e:
        raise Exception(f"Error sanitizing tickers: {e}")

def update_style_from_categorization_etfs() -> None:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE public.ticker t
                    SET style_type = ce.style_type,
                        cap_type   = ce.cap_type,
                        type_from  = 'CAT_ETF'
                    FROM public.categorize_etf_holding ceh
                    JOIN public.categorize_etf ce ON ce.id = ceh.categorize_etf_id
                    WHERE t.id       = ceh.ticker_id
                      AND ce.usage   = 'style'
                      AND ce.style_type IS NOT NULL
                      AND t.invalid  IS NULL;
                """)
    except Error as e:
        raise Exception(f"Error updating ticker style from categorization ETFs: {e}")

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
                      AND pe.style_type IN ('value', 'growth');
                """)
    except Error as e:
        raise Exception(f"Error updating ticker style from provider ETFs: {e}")

