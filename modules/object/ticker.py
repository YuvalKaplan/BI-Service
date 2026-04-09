import log
from datetime import datetime
from psycopg.errors import Error
from psycopg.rows import class_row
from dataclasses import dataclass
from modules.core.db import db_pool_instance
from modules.calc.classification import StyleClassifier

@dataclass
class Ticker:
    symbol: str
    created_at: datetime | None
    source: str | None
    style_type: str | None
    cap_type: str | None
    type_from: str | None
    isin: str | None
    cik: str | None
    exchange: str | None
    name: str | None
    industry: str | None
    sector: str | None
    invalid: str | None

    def __init__(self, symbol: str, created_at: datetime | None = None, 
                 source: str | None = None, style_type: str | None = None, cap_type: str | None = None, type_from: str | None = None, 
                 isin: str | None = None, cik: str | None = None, exchange: str | None = None, 
                 name: str | None = None, industry: str | None = None, sector: str | None = None, invalid: str | None = None):
        self.symbol = symbol 
        self.created_at = created_at
        self.source = source
        self.style_type = style_type
        self.cap_type = cap_type
        self.type_from = type_from
        self.isin = isin
        self.cik = cik
        self.exchange = exchange
        self.name = name
        self.industry = industry
        self.sector = sector
        self.invalid = invalid


def fetch_by_symbol(symbol: str):
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Ticker)) as cur:
                cur.execute('SELECT * FROM ticker WHERE symbol = %s;', (symbol,))
                item = cur.fetchone()
        return item
    except Error as e:
        raise Exception(f"Error fetching the Ticker from the DB: {e}")

def fetch_by_symbols(symbols: list[str]):
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Ticker)) as cur:
                query_str = """
                    SELECT * FROM ticker
                    WHERE symbol = ANY(%s);
                """
                cur.execute(query_str, (symbols,))
                items = cur.fetchall()
        return items
    except Error as e:
        raise Exception(f"Error fetching the Ticker list page from the DB: {e}")

def upsert_tickers_style_and_cap(symbols: list[str], cap_type: str, style_type: str):
    if not symbols:
        return

    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                query = """
                    INSERT INTO ticker (symbol, cap_type, style_type, source, type_from)
                    SELECT unnest(%s::text[]), %s, %s, 'provider_etf', 'ETF'
                    ON CONFLICT (symbol)
                    DO UPDATE
                    SET
                        cap_type = EXCLUDED.cap_type,
                        style_type = EXCLUDED.style_type,
                        style_type = 'provider_etf'
                        type_from = 'ETF'
                    WHERE ticker.cap_type IS DISTINCT FROM EXCLUDED.cap_type
                       OR ticker.style_type IS DISTINCT FROM EXCLUDED.style_type;
                """

                cur.execute(query, (symbols, cap_type, style_type))

    except Error as e:
        raise Exception(f"Error upserting Tickers: {e}")

def update_info(item: Ticker):
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                insert_query = "UPDATE ticker SET isin =%s, cik = %s, name = %s, industry = %s, sector = %s WHERE symbol = %s;"
                cur.execute(insert_query, (item.isin, item.cik, item.name, item.industry, item.sector, item.symbol))

    except Error as e:
        raise Exception(f"Error updating the Ticker item into the DB: {e}")

def update_invalid(symbol: str, reason: str):
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                insert_query = "UPDATE ticker SET invalid =%s WHERE symbol = %s;"
                cur.execute(insert_query, (reason, symbol))

    except Error as e:
        raise Exception(f"Error updating the Ticker invalid reason into the DB: {e}")


def mark_style(classifier: StyleClassifier) -> None:
    try:
        with db_pool_instance.get_connection() as conn:
            # 1. Update from ETF categorization
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE public.ticker t
                    SET
                        style_type = c.style_type,
                        type_from = 'ETF'
                    FROM public.categorize_ticker c
                    WHERE t.symbol = c.symbol
                    AND c.style_type IS NOT NULL;
                """)
            conn.commit()

            # 2. Find symbols still missing a style classification
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT symbol
                    FROM public.ticker
                    WHERE style_type IS NULL
                      AND invalid IS NULL
                """)
                missing_symbols = [row[0] for row in cur.fetchall()]
            conn.commit()

            log.record_status(f"Classification using model needed for {len(missing_symbols)} tickers")

            # 3. Classify remaining symbols via the ML model
            results = classifier.classify_symbols(missing_symbols)

            updates = [(r["style"], "MODEL", r["symbol"]) for r in results]
            with conn.cursor() as cur:
                cur.executemany("""
                    UPDATE public.ticker
                    SET style_type = %s,
                        type_from = %s
                    WHERE symbol = %s
                """, updates)
            conn.commit()

    except Error as e:
        raise Exception(f"Error marking style for tickers in the DB: {e}")
