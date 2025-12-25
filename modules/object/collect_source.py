from datetime import datetime, timezone
from psycopg.errors import Error
from psycopg.rows import class_row, dict_row
from dataclasses import dataclass
from modules.core.db import db_pool_instance

GAP_DAYS_BETWEEN_SCRAPES = 7

@dataclass
class CollectSource:
    id: int | None
    created_at: datetime | None
    domain: str | None
    url: str | None
    wait_on_selector: str | None
    content_selector: str | None
    events: dict | None
    cookies: dict | None
    scrape_levels: int | None
    frequency: str | None
    last_scrape: datetime | None
    disabled: bool | None
    disabled_reason: str | None

    def __init__(self, id: int | None = None, created_at: datetime | None = None, domain: str | None = None, url: str | None = None, 
                 wait_on_selector: str | None = None, content_selector: str | None = None, events: dict | None = None, cookies: dict | None = None, scrape_levels: int | None = None, 
                 frequency: str | None = None, last_scrape: datetime | None = None, disabled: bool | None = False, disabled_reason: str | None = None):
        self.id = id 
        self.created_at = created_at
        self.domain = domain
        self.url = url
        self.wait_on_selector = wait_on_selector
        self.content_selector = content_selector
        self.events = events
        self.cookies = cookies
        self.scrape_levels = scrape_levels
        self.frequency = frequency
        self.last_scrape = last_scrape
        self.disabled = disabled
        self.disabled_reason = disabled_reason

def fetch_by_id(id: int):
    with db_pool_instance.get_connection() as conn:
        with conn.cursor(row_factory=class_row(CollectSource)) as cur:
            cur.execute('SELECT * FROM collect_source WHERE id = %s;', (id,))
            item = cur.fetchone()
    return item

def fetch_by_url(url: str):
    with db_pool_instance.get_connection() as conn:
        with conn.cursor(row_factory=class_row(CollectSource)) as cur:
            cur.execute('SELECT * FROM collect_source WHERE url = %s;', (url,))
            item = cur.fetchone()
    return item

def fetch_by_ids(ids: list[int]):
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(CollectSource)) as cur:
                query_str = """
                    SELECT * FROM collect_source
                    WHERE id = ANY(%s)
                    ;
                """
                cur.execute(query_str, (ids,))
                items = cur.fetchall()
        return items
    except Error as e:
        raise Exception(f"Error fetching the CollectSource list page from the DB: {e}")


def fetch_for_scraping(limit: int):
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=class_row(CollectSource)) as cur:
                query_str = """
                    SELECT * FROM collect_source
                    WHERE
                    disabled = false
                    AND (
                        (last_scrape IS NULL) 
                        OR
                        (frequency = 'daily' AND last_scrape <= NOW() - INTERVAL '1 day') OR
                        (frequency = 'weekly' AND last_scrape <= NOW() - INTERVAL '3 day') OR
                        (frequency = 'every_2_weeks' AND last_scrape <= NOW() - INTERVAL '1 week') OR
                        (frequency = 'monthly' AND last_scrape <= NOW() - INTERVAL '2 week'))
                    LIMIT %s;
                """
                cur.execute(query_str, (limit,))
                items = cur.fetchall()
        return items
    except Error as e:
        raise Exception(f"Error fetching the CollectSource list page from the DB: {e}")


def get_collection_stats(ids: list[int], start: datetime) -> list[dict]:
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                query_str = """
                    SELECT cs.domain, COUNT(a.id) AS count 
                    FROM collect_source as cs
                    LEFT OUTER JOIN public.analysis as a ON cs.id = a.collect_source_id AND a.created_at >= %s
                    WHERE cs.id = ANY(%s)
                    GROUP BY cs.domain
                    ORDER BY count
                """
                cur.execute(query_str, (start, ids))
                items = cur.fetchall()
        return items
    except Error as e:
        raise Exception(f"Error fetching the CollectSource list page from the DB: {e}")

def update_domain(item: CollectSource):
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                insert_query = "UPDATE collect_source SET domain = %s WHERE id = %s;"
                cur.execute(insert_query, (item.domain, item.id))
                conn.commit()
                return

        raise Exception(f"Failed to update last scraped")
    
    except Error as e:
        raise Exception(f"Error updating the CollectSource item into the DB: {e}")

def update_last_scrape(item: CollectSource):
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                insert_query = "UPDATE collect_source SET last_scrape = %s WHERE id = %s;"
                cur.execute(insert_query, (datetime.now(timezone.utc), item.id))
                conn.commit()
                return

        raise Exception(f"Failed to update last scrape")
    
    except Error as e:
        raise Exception(f"Error updating the CollectSource item into the DB: {e}")
