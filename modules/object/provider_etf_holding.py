from typing import List
from datetime import date, datetime
from psycopg.errors import Error
from psycopg.rows import class_row
from dataclasses import dataclass
from modules.core.db import db_pool_instance
import pandas as pd

@dataclass
class ProviderEtfHolding:
    id: int
    created_at: datetime
    provider_etf_id: int
    holding_date: date
    ticker_id: int | None
    shares: float
    market_value: float
    weight: float

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


def insert_all_holdings(etf_id: int, df: pd.DataFrame) -> None:
    try:
        df = df.drop(columns=["id"], errors="ignore")
        df["provider_etf_id"] = etf_id
        df = df[[
            "provider_etf_id",
            "holding_date",
            "ticker_id",
            "shares",
            "market_value",
            "weight"
        ]]
        rows = list(df.itertuples(index=False, name=None)).copy()

        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                delete_query = """
                    DELETE FROM provider_etf_holding peh
                    WHERE peh.provider_etf_id = %s
                    AND peh.holding_date = %s;
                """
                cur.execute(delete_query, (etf_id, df["holding_date"].iat[0]))
                insert_query = """
                    INSERT INTO provider_etf_holding
                        (provider_etf_id, holding_date, ticker_id, shares, market_value, weight)
                    VALUES (%s, %s, %s, %s, %s, %s);
                """
                cur.executemany(insert_query, rows)

    except Error as e:
        raise Exception(f"Error inserting the Provider ETF Holdings into the DB: {e}")
