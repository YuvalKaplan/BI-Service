"""
EDGAR access for the SEC modules. Every request carries the User-Agent the SEC's fair-access
policy requires (SECRET_SEC_USER_AGENT, e.g. "BI-Service admin@example.com" - requests without one are
refused with 403), asks for gzip, and is throttled to half the SEC's 10 requests per second.
"""
import gzip
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import date
from threading import Lock
from urllib.error import HTTPError
from urllib.request import Request, urlopen

EDGAR_ARCHIVES_URL = 'https://www.sec.gov/Archives/edgar'
MIN_REQUEST_INTERVAL = 0.2  # seconds between requests: 5 per second
API_RETRIES = 3
API_RETRY_DELAY = 2.0       # seconds; doubles on each subsequent attempt
REQUEST_TIMEOUT = 60        # seconds

_last_request = 0.0
_throttle_lock = Lock()


@dataclass
class IndexEntry:
    """One filing in EDGAR's quarterly form index."""
    form_type: str
    company: str
    cik: str
    filed: date
    accession: str  # 0000894189-25-016878


def _throttle() -> None:
    global _last_request
    with _throttle_lock:
        wait = MIN_REQUEST_INTERVAL - (time.monotonic() - _last_request)
        if wait > 0:
            time.sleep(wait)
        _last_request = time.monotonic()


def _user_agent() -> str:
    user_agent = os.getenv('SECRET_SEC_USER_AGENT')
    if not user_agent:
        raise Exception("SECRET_SEC_USER_AGENT is not set - the SEC refuses requests without a declared User-Agent "
                        "with a contact email (e.g. 'BI-Service admin@example.com')")
    return user_agent


def error_page_heading(e: HTTPError) -> str:
    """The heading of the SEC's error page behind an HTTPError."""
    try:
        body = e.read()
        if e.headers.get('Content-Encoding') == 'gzip':
            body = gzip.decompress(body)
    except Exception:
        return 'no error page'
    return page_heading(body.decode('utf-8', 'replace'))


def page_heading(page: str) -> str:
    """The heading of an SEC error page, which says why it refused: "Your Request Originates from
    an Undeclared Automated Tool" (the User-Agent, or the IP taken for a bot), "Request Rate
    Threshold Exceeded", or "Access Denied" (an IP block at the SEC's CDN)."""
    heading = re.search(r'<h1[^>]*>(.*?)</h1>', page, re.S | re.I)
    text = heading.group(1) if heading else re.sub(r'(?s)<(script|style).*?</\1>', '', page)
    return re.sub(r'\s+', ' ', re.sub(r'<[^>]+>', ' ', text)).strip()[:200] or 'empty error page'


def get(url: str) -> bytes:
    """The body at url, gunzipped. Retries server errors and timeouts; a 404 raises the HTTPError
    at once, and so does a 403 (the SEC refusing the User-Agent or blocking the IP), with the SEC
    page's heading - retrying can't help and only prolongs an IP block."""
    headers = {'User-Agent': _user_agent(), 'Accept-Encoding': 'gzip'}
    last_exc: Exception | None = None
    for attempt in range(API_RETRIES):
        _throttle()
        try:
            with urlopen(Request(url, headers=headers), timeout=REQUEST_TIMEOUT) as response:
                body = response.read()
                return gzip.decompress(body) if response.headers.get('Content-Encoding') == 'gzip' else body
        except HTTPError as e:
            if e.code == 404:
                raise
            if e.code == 403:
                raise Exception(f"EDGAR refused the request ({url}): HTTP Error 403: {error_page_heading(e)}") from e
            last_exc = e
        except Exception as e:
            last_exc = e
        if attempt < API_RETRIES - 1:
            time.sleep(API_RETRY_DELAY * (2 ** attempt))
    raise Exception(f"EDGAR request failed after {API_RETRIES} attempts ({url}): {last_exc}")


def form_index(year: int, quarter: int, form_types: tuple[str, ...]) -> list[IndexEntry]:
    """The filings of the given form types in EDGAR's form index for a quarter (updated nightly
    for the current one). [] when the quarter's index doesn't exist yet."""
    try:
        text = get(f"{EDGAR_ARCHIVES_URL}/full-index/{year}/QTR{quarter}/form.idx").decode('latin-1')
    except HTTPError as e:
        if e.code == 404:
            return []
        raise

    # Rows are "<form type>  <company>  <cik>  <date filed>  edgar/data/<cik>/<accession>.txt".
    # The columns are wider than the header suggests, so a row is read from the right; form
    # type and company are separated by two or more spaces (company names have single ones).
    entries = []
    for line in text.splitlines():
        if not line.startswith(form_types):
            continue
        parts = line.rsplit(None, 3)
        if len(parts) != 4 or not parts[3].startswith('edgar/'):
            continue
        rest, cik, filed, path = parts
        form_type, *company = re.split(r'\s{2,}', rest.strip(), maxsplit=1)
        if form_type not in form_types:
            continue
        entries.append(IndexEntry(
            form_type=form_type,
            company=company[0].strip() if company else '',
            cik=cik,
            filed=date.fromisoformat(filed),
            accession=path.rsplit('/', 1)[-1].removesuffix('.txt'),
        ))
    return entries


def filing_xml(cik: str, accession: str) -> bytes:
    """A filing's XML document: primary_doc.xml, else the first .xml its folder index lists."""
    folder = f"{EDGAR_ARCHIVES_URL}/data/{int(cik)}/{accession.replace('-', '')}"
    try:
        return get(f"{folder}/primary_doc.xml")
    except HTTPError as e:
        if e.code != 404:
            raise
    listing = json.loads(get(f"{folder}/index.json"))
    names = [item['name'] for item in listing.get('directory', {}).get('item', [])
             if item.get('name', '').lower().endswith('.xml')]
    if not names:
        raise Exception(f"No XML document in filing {accession}")
    return get(f"{folder}/{names[0]}")
