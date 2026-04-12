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
    esg_qualified: bool | None
    invalid: str | None

    def __init__(self, symbol: str, created_at: datetime | None = None,
                 source: str | None = None, style_type: str | None = None, cap_type: str | None = None, type_from: str | None = None,
                 isin: str | None = None, cik: str | None = None, exchange: str | None = None,
                 name: str | None = None, industry: str | None = None, sector: str | None = None,
                 esg_qualified: bool | None = None, invalid: str | None = None):
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
        self.esg_qualified = esg_qualified
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

def upsert(item: Ticker):
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                query = """
                    INSERT INTO ticker (symbol, isin, cik, name, exchange, industry, sector, source, type_from)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol)
                    DO UPDATE
                    SET isin = EXCLUDED.isin, 
                        cik = EXCLUDED.cik, 
                        name = EXCLUDED.name, 
                        exchange = EXCLUDED.exchange, 
                        industry = EXCLUDED.industry, 
                        sector = EXCLUDED.sector,
                        source = EXCLUDED.source,
                        type_from = EXCLUDED.type_from;
                """
                cur.execute(query, (item.symbol, item.isin, item.cik, item.name, item.exchange, item.industry, item.sector, item.source, item.type_from))
    
    except Error as e:
        raise Exception(f"Error upserting ticker into the DB: {e}")

def update_esg_qualified(symbols: list[str]):
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

def update_invalid(symbol: str, reason: str):
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                insert_query = "UPDATE ticker SET invalid =%s WHERE symbol = %s;"
                cur.execute(insert_query, (reason, symbol))

    except Error as e:
        raise Exception(f"Error updating the Ticker invalid reason into the DB: {e}")


def mark_style(classifier: StyleClassifier) -> int:
    try:
        with db_pool_instance.get_connection() as conn:
            # Update from ETF categorization
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE public.ticker t
                    SET
                        style_type = c.style_type,
                        type_from = 'CAT_ETF'
                    FROM public.categorize_ticker c
                    WHERE t.symbol = c.symbol
                    AND c.style_type IS NOT NULL;
                """)
            conn.commit()
            
            # Find symbols still missing a style classification
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

            # Classify remaining symbols via the ML model
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

            return len(missing_symbols)

    except Error as e:
        raise Exception(f"Error marking style for tickers in the DB: {e}")
