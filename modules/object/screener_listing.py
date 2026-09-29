from datetime import date
from typing import List
from dataclasses import dataclass
from psycopg import sql
from psycopg.errors import Error
from psycopg.rows import class_row
from modules.core.db import db_pool_instance

LINE_HOME = 'home'              # the company's home market, a US exchange, or an unscreened domicile — registered by the screener
LINE_FOREIGN = 'foreign'        # outside the company's home market — decided by the company builder, after the master sync
LINE_NON_EQUITY = 'non_equity'  # preferred / note / warrant / unit / participation line — never used
LINE_ORDER_BOOK = 'order_book'  # LSE International Order Book mirror — never used


@dataclass
class ScreenerListing:
    """
    One line of the FMP large-cap screener on a screen date, as returned (symbol with its
    exchange suffix, e.g. TD.TO; market cap and price in the listing's local currency), with its
    line_type and, once registered, its ticker_id. Written by modules/cron/screener.py,
    read by modules/cron/company_builder.py and the sim benchmark backfill.
    """
    screen_date: date
    symbol: str
    exchange: str
    company_name: str | None
    country: str | None
    market_cap: float | None
    price: float | None
    line_type: str
    ticker_id: int | None = None

    @property
    def quote_shares(self) -> float | None:
        """The listing's share count in FMP's live quote — the glitch filter's reference."""
        return self.market_cap / self.price if self.market_cap and self.price and self.price > 0 else None


_COLUMNS = ('screen_date', 'symbol', 'exchange', 'company_name', 'country', 'market_cap', 'price', 'line_type', 'ticker_id')
_INSERT_SQL = sql.SQL("INSERT INTO screener_listing ({columns}) VALUES ({placeholders});").format(
    columns=sql.SQL(", ").join(map(sql.Identifier, _COLUMNS)),
    placeholders=sql.SQL(", ").join(sql.Placeholder() for _ in _COLUMNS),
)


def replace_for_date(screen_date: date, items: List[ScreenerListing]) -> None:
    """Replaces the screen stored for screen_date with `items`."""
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('DELETE FROM screener_listing WHERE screen_date = %s;', (screen_date,))
                if items:
                    cur.executemany(_INSERT_SQL, [tuple(getattr(i, c) for c in _COLUMNS) for i in items])
            conn.commit()
    except Error as e:
        raise Exception(f"Error replacing screener listings for {screen_date}: {e}")


def fetch_for_date(screen_date: date) -> List[ScreenerListing]:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(ScreenerListing)) as cur:
                cur.execute(
                    f"SELECT {', '.join(_COLUMNS)} FROM screener_listing WHERE screen_date = %s ORDER BY exchange, symbol;",
                    (screen_date,),
                )
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error fetching screener listings for {screen_date}: {e}")


def fetch_latest_date(up_to: date | None = None) -> date | None:
    """The latest screen date on or before up_to (default: any)."""
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                if up_to is None:
                    cur.execute('SELECT MAX(screen_date) FROM screener_listing;')
                else:
                    cur.execute('SELECT MAX(screen_date) FROM screener_listing WHERE screen_date <= %s;', (up_to,))
                row = cur.fetchone()
                return row[0] if row else None
    except Error as e:
        raise Exception(f"Error fetching the latest screen date: {e}")


def set_ticker_ids(screen_date: date, items: List[ScreenerListing]) -> None:
    """Saves each item's ticker_id on its stored line."""
    if not items:
        return
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    'UPDATE screener_listing SET ticker_id = %s WHERE screen_date = %s AND symbol = %s AND exchange = %s;',
                    [(i.ticker_id, screen_date, i.symbol, i.exchange) for i in items],
                )
            conn.commit()
    except Error as e:
        raise Exception(f"Error saving screener listing ticker ids for {screen_date}: {e}")
