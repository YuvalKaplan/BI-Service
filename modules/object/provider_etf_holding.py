from typing import List
from datetime import date, datetime
from psycopg.errors import Error
from psycopg.rows import class_row
from dataclasses import dataclass
from modules.core.db import db_pool_instance

@dataclass
class ProviderEtfHolding:
    """One holding line of a provider ETF as FMP lists it (modules/cron/etf_downloader.py).
    ticker_id is the ticker the line resolved to; FMP's symbol / name / ISIN / CUSIP identify the
    line (and the unresolved ones). weight is a fraction."""
    id: int | None
    created_at: datetime | None
    provider_etf_id: int
    holding_date: date
    ticker_id: int | None
    shares: float | None
    market_value: float | None
    weight: float | None
    symbol: str | None = None
    name: str | None = None
    isin: str | None = None
    cusip: str | None = None

def fetch_valid_ticker_ids_in_holdings() -> List[int]:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT peh.ticker_id
                    FROM provider_etf_holding AS peh
                    JOIN ticker AS t ON peh.ticker_id = t.id
                    WHERE t.invalid IS NULL
                """)
                return [row[0] for row in cur.fetchall()]
    except Error as e:
        raise Exception(f"Error retrieving valid ticker IDs in holdings: {e}")


def fetch_valid_ticker_ids_in_recent_holdings(look_back_days: int) -> List[int]:
    """Valid tickers in each active ETF's latest holdings from the last look_back_days — the
    holdings best ideas can use (best_ideas_generator.LOOK_BACK_WINDOW)."""
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    WITH latest AS (
                        SELECT peh.provider_etf_id, MAX(peh.holding_date) AS holding_date
                        FROM provider_etf_holding AS peh
                        JOIN provider_etf AS pe ON pe.id = peh.provider_etf_id AND pe.status = 'active'
                        WHERE peh.holding_date > NOW() - (%s * INTERVAL '1 day')
                        GROUP BY peh.provider_etf_id
                    )
                    SELECT DISTINCT peh.ticker_id
                    FROM provider_etf_holding AS peh
                    JOIN latest AS l ON l.provider_etf_id = peh.provider_etf_id AND l.holding_date = peh.holding_date
                    JOIN ticker AS t ON t.id = peh.ticker_id
                    WHERE t.invalid IS NULL;
                """, (look_back_days,))
                return [row[0] for row in cur.fetchall()]
    except Error as e:
        raise Exception(f"Error retrieving valid ticker IDs in recent holdings: {e}")


def fetch_valid_tickers_in_holdings() -> List[str]:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                query = """
                    SELECT DISTINCT t.symbol
                    FROM public.provider_etf_holding AS peh
                    LEFT JOIN ticker AS t ON peh.ticker_id = t.id
                    WHERE t.invalid IS NULL
                    ORDER BY t.symbol
                """
                cur.execute(query)
                return [row[0] for row in cur.fetchall()]
    except Error as e:
        raise Exception(f"Error retrieving valid tickers in holdings: {e}")

def fetch_valid_holdings_by_provider_etf_id(provider_etf_id: int, holding_date: date) -> List[ProviderEtfHolding]:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(ProviderEtfHolding)) as cur:
                query_str = """
                    SELECT peh.*
                    FROM provider_etf_holding AS peh
                    INNER JOIN ticker AS t ON peh.ticker_id = t.id
                    WHERE t.invalid IS NULL
                      AND peh.provider_etf_id = %s
                      AND peh.holding_date = %s;
                """
                cur.execute(query_str, (provider_etf_id, holding_date))
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error fetching holdings for provider ETF ID from the DB: {e}")


def fetch_latest_holdings_for_etf(provider_etf_id: int, look_back_days: int, up_to_date: date | None = None) -> List[ProviderEtfHolding]:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(ProviderEtfHolding)) as cur:
                if up_to_date is None:
                    query_str = """
                        SELECT peh.*
                        FROM provider_etf_holding AS peh
                        INNER JOIN ticker AS t ON peh.ticker_id = t.id
                        WHERE t.invalid IS NULL
                          AND peh.provider_etf_id = %s
                          AND peh.holding_date = (
                              SELECT MAX(holding_date)
                              FROM provider_etf_holding
                              WHERE provider_etf_id = %s
                                AND holding_date > NOW() - (%s * INTERVAL '1 day')
                          )
                        ORDER BY peh.id;
                    """
                    cur.execute(query_str, (provider_etf_id, provider_etf_id, look_back_days))
                else:
                    query_str = """
                        SELECT peh.*
                        FROM provider_etf_holding AS peh
                        INNER JOIN ticker AS t ON peh.ticker_id = t.id
                        WHERE t.invalid IS NULL
                          AND peh.provider_etf_id = %s
                          AND peh.holding_date = (
                              SELECT MAX(holding_date)
                              FROM provider_etf_holding
                              WHERE provider_etf_id = %s
                                AND holding_date <= %s
                                AND holding_date > %s - (%s * INTERVAL '1 day')
                          )
                        ORDER BY peh.id;
                    """
                    cur.execute(query_str, (provider_etf_id, provider_etf_id, up_to_date, up_to_date, look_back_days))
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error fetching latest holdings for provider ETF {provider_etf_id}: {e}")


# Max/min ratio of implied prices (market_value / shares) under which duplicate lines of one
# ticker are treated as lots of the same security rather than different securities.
DUP_PRICE_TOLERANCE = 1.02


@dataclass
class QuarantinedHolding:
    ticker_id: int
    lines: List[ProviderEtfHolding]
    implied_prices: List[float | None]


def implied_price(h: ProviderEtfHolding) -> float | None:
    if h.shares and h.market_value and h.shares > 0 and h.market_value > 0:
        return h.market_value / h.shares
    return None


def aggregate_holdings(raw: List[ProviderEtfHolding]) -> tuple[List[ProviderEtfHolding], List[QuarantinedHolding]]:
    """
    Collapses an ETF's holding lines to one holding per ticker_id. Providers sometimes list a
    ticker on more than one line: when every line implies the same price (within
    DUP_PRICE_TOLERANCE) they're lots of one security, so shares/market_value/weight are summed
    into a single holding that keeps the lowest line id. When the implied prices disagree (or
    can't be computed) the lines are most likely different securities that ticker resolution
    mapped onto the same ticker — there's no telling which one is right, so all of that
    ticker's lines are quarantined and returned separately instead of being used.
    Expects `raw` ordered by id (fetch_latest_holdings_for_etf does this) so output is stable.
    """
    groups: dict[int, List[ProviderEtfHolding]] = {}
    order: List[int] = []
    passthrough: List[ProviderEtfHolding] = []
    for h in raw:
        if h.ticker_id is None:
            passthrough.append(h)
            continue
        if h.ticker_id not in groups:
            groups[h.ticker_id] = []
            order.append(h.ticker_id)
        groups[h.ticker_id].append(h)

    holdings: List[ProviderEtfHolding] = []
    quarantined: List[QuarantinedHolding] = []
    for ticker_id in order:
        lines = groups[ticker_id]
        if len(lines) == 1:
            holdings.append(lines[0])
            continue

        prices = [implied_price(h) for h in lines]
        valid = [p for p in prices if p is not None]
        if len(valid) != len(prices) or max(valid) / min(valid) > DUP_PRICE_TOLERANCE:
            quarantined.append(QuarantinedHolding(ticker_id=ticker_id, lines=lines, implied_prices=prices))
            continue

        first = min(lines, key=lambda h: h.id)
        holdings.append(ProviderEtfHolding(
            id=first.id,
            created_at=first.created_at,
            provider_etf_id=first.provider_etf_id,
            holding_date=first.holding_date,
            ticker_id=ticker_id,
            shares=sum(h.shares or 0 for h in lines),
            market_value=sum(h.market_value or 0 for h in lines),
            weight=sum(h.weight or 0 for h in lines),
            symbol=first.symbol,
            name=first.name,
            isin=first.isin,
            cusip=first.cusip,
        ))

    return holdings + passthrough, quarantined


def fetch_max_holding_date() -> date | None:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT MAX(holding_date) FROM provider_etf_holding;")
                row = cur.fetchone()
                if not row or row[0] is None:
                    return None
                value = row[0]
                # holding_date is stored as `timestamp without time zone` in the DB despite
                # the dataclass typing it as `date` — normalize so callers can compare/step
                # this against plain date objects without a datetime/date TypeError.
                return value.date() if isinstance(value, datetime) else value
    except Error as e:
        raise Exception(f"Error fetching max holding date: {e}")


def fetch_latest_dates() -> dict[int, date]:
    """Each ETF's latest holdings date - the selection's freshness rule."""
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT provider_etf_id, MAX(holding_date) FROM provider_etf_holding GROUP BY provider_etf_id;")
                return {row[0]: (row[1].date() if isinstance(row[1], datetime) else row[1]) for row in cur.fetchall()}
    except Error as e:
        raise Exception(f"Error fetching the provider ETFs' latest holdings dates: {e}")


def fetch_latest_lines(provider_etf_id: int) -> List[ProviderEtfHolding]:
    """Every line (resolved or not) of the ETF's latest stored holdings."""
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(ProviderEtfHolding)) as cur:
                cur.execute("""
                    SELECT * FROM provider_etf_holding
                    WHERE provider_etf_id = %s
                      AND holding_date = (SELECT MAX(holding_date) FROM provider_etf_holding WHERE provider_etf_id = %s)
                    ORDER BY id;
                """, (provider_etf_id, provider_etf_id))
                return cur.fetchall()
    except Error as e:
        raise Exception(f"Error fetching the latest holdings of provider ETF {provider_etf_id}: {e}")


_INSERT_COLUMNS = ('provider_etf_id', 'holding_date', 'ticker_id', 'shares', 'market_value', 'weight',
                   'symbol', 'name', 'isin', 'cusip')


def replace_holdings(provider_etf_id: int, holding_date: date, lines: List[ProviderEtfHolding]) -> None:
    """Stores the ETF's lines for holding_date, replacing any stored for that date."""
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM provider_etf_holding WHERE provider_etf_id = %s AND holding_date = %s;",
                            (provider_etf_id, holding_date))
                cur.executemany(
                    f"INSERT INTO provider_etf_holding ({', '.join(_INSERT_COLUMNS)}) "
                    f"VALUES ({', '.join(['%s'] * len(_INSERT_COLUMNS))});",
                    [tuple(getattr(h, c) for c in _INSERT_COLUMNS) for h in lines],
                )
            conn.commit()
    except Error as e:
        raise Exception(f"Error saving the holdings of provider ETF {provider_etf_id}: {e}")
