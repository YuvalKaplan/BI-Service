from datetime import date, datetime
from psycopg.errors import Error
from psycopg.rows import class_row
from dataclasses import dataclass
import log
from modules.core.db import db_pool_instance_bt
from modules.bt.calc.classification import StyleClassifier

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
        with db_pool_instance_bt.get_connection() as conn:
            with conn.cursor(row_factory=class_row(Ticker)) as cur:
                cur.execute('SELECT * FROM ticker WHERE symbol = %s;', (symbol,))
                item = cur.fetchone()
        return item
    except Error as e:
        raise Exception(f"Error fetching the Ticker from the DB: {e}")

def fetch_by_symbols(symbols: list[str]):
    try:
        with db_pool_instance_bt.get_connection() as conn:
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

def update_info(item: Ticker):
    try:
        with db_pool_instance_bt.get_connection() as conn:
            with conn.cursor() as cur:
                insert_query = "UPDATE ticker SET isin =%s, cik = %s, name = %s, industry = %s, sector = %s WHERE symbol = %s;"
                cur.execute(insert_query, (item.isin, item.cik, item.name, item.industry, item.sector, item.symbol))

    except Error as e:
        raise Exception(f"Error updating the Ticker item into the DB: {e}")

def update_esg_qualified(symbols: list[str]):
    if not symbols:
        return
    try:
        with db_pool_instance_bt.get_connection() as conn:
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
        with db_pool_instance_bt.get_connection() as conn:
            with conn.cursor() as cur:
                query = """
                    INSERT INTO ticker (symbol, invalid)
                    VALUES (%s, %s)
                    ON CONFLICT (symbol)
                    DO UPDATE
                    SET invalid = EXCLUDED.invalid;
                """
                cur.execute(query, (symbol, reason))
    
    except Error as e:
        raise Exception(f"Error updating ticker invalid reason into the DB: {e}")


def upsert(item: Ticker):
    try:
        with db_pool_instance_bt.get_connection() as conn:
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


def mark_style(classifier: StyleClassifier) -> int:
    try:
        with db_pool_instance_bt.get_connection() as conn:
            # 1. Update from ETF categorization
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

            # 2. Get symbols missing classification
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

            # 3. Classify missing symbols
            results = classifier.classify_symbols(missing_symbols)

            updates = [(r["style"], "MODEL", r["symbol"]) for r in results]
            with conn.cursor() as cur:
                update_sql = """
                    UPDATE public.ticker
                    SET style_type = %s,
                        type_from = %s
                    WHERE symbol = %s
                """
                cur.executemany(update_sql, updates)

            conn.commit()

        return len(missing_symbols)
    except Error as e:
        raise Exception(f"Error marking the categories for the tickers in the DB: {e}")
    

              
def mark_split_invalid(symbols: list[str], start_date: date, end_date: date):
    try:
        with db_pool_instance_bt.get_connection() as conn:
            # First remove priod invalid over split (could have been different period)
            with conn.cursor() as cur:
                cur.execute("""UPDATE public.ticker SET invalid = NULL WHERE invalid = 'Split in test period'""")

            with conn.cursor() as cur:
                query = """
                    UPDATE public.ticker 
                    SET invalid = 'Split in test period'
                    WHERE ticker.invalid IS NULL 
                    AND ticker.symbol = ANY(%s)
                    AND ticker.symbol IN (
                        SELECT tsh.symbol 
                        FROM public.ticker_split_history tsh
                        WHERE tsh.date >= %s AND tsh.date <= %s
                    );
                """
                cur.execute(query, (symbols, start_date, end_date,))
        return
    except Error as e:
        raise Exception(f"Error marking the categories for the tickers in the DB: {e}")


def sanitize():
    """Mark any ticker table rows with non-standard symbols as invalid.

    Delegates entirely to the sanitize_tickers() SQL function.
    """
    try:
        with db_pool_instance_bt.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT sanitize_tickers();')
    except Error as e:
        raise Exception(f"Error sanitizing tickers: {e}")
