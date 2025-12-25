import os
import log
import re
import tempfile
from typing import List, Tuple, Any
from dataclasses import dataclass
from urllib.parse import urlparse
from typing import Optional
from bs4 import BeautifulSoup, Comment
from bs4.element import Tag
from playwright.sync_api import sync_playwright, Download, Browser, BrowserContext, Page
from playwright_stealth import Stealth
from modules.object.file import File

ENV_TYPE = os.environ.get("ENV_TYPE")

# A common, modern user-agent string
REAL_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

@dataclass
class OpenBrowser:
    browser: Browser | None
    context: BrowserContext | None
    page: Page | None

@dataclass
class CleanHtml:
    anchor_map: dict[str, str]
    html_anchor_mapped: str
    urls_with_specific_text: List[str]
    html: str
    text: str

def get_final_url(initial_url: str) -> str | None:
    """
    Resolves redirects using an event listener, which is a cleaner pattern.
    """
    
    # This list will hold the download object if one is triggered.
    # We use a list because it's a mutable object that the
    # inner 'on_download' function can modify.
    download_event = []

    def on_download(download: Download):
        """Callback function when a download starts."""
        download_event.append(download)

    with Stealth().use_sync(sync_playwright()) as p:
        browser = p.chromium.launch(headless=True,
            # Add extra launch arguments to mimic real Chrome
            args=[
            "--disable-blink-features=AutomationControlled",
            "--disable-infobars",
            "--disable-web-security",
            "--disable-site-isolation-trials",
            "--disable-dev-shm-usage",
            "--no-sandbox",
            "--ignore-certificate-errors"
        ])

        context = browser.new_context(
            user_agent=REAL_USER_AGENT,
            viewport={'width': 1920, 'height': 1080}
        )
                
        page = context.new_page()

        # 1. Register the download event listener *before* navigation.
        page.on("download", on_download)

        final_url = None
        try:
            # 2. Navigate. We wait for 'networkidle' to let JS redirects finish.
            # This call will succeed for HTML pages.
            # It will fail (and throw an error) for downloads,
            # but that's okay!
            page.goto(initial_url, timeout=20000, wait_until="domcontentloaded")

            # 3. Add a manual JS wait / redirect detection loop
            # Some links redirect asynchronously or via window.location.assign after delayed scripts.
            page.wait_for_timeout(5000)
            if "chrome-error://" in page.url:
                page.reload()
                page.wait_for_timeout(3000)

            # 4. If goto() succeeds, the final URL is the page's URL.
            final_url = page.url
            
        except Exception as e:
            # 5. If goto() fails (e.g., net::ERR_ABORTED), it's *probably*
            #    because a download was triggered instead of a navigation.
            #    We don't need to do anything here yet, because we'll
            #    check our download_event list.
            #    Sometimes download takes some time to start and set the download_event
            #    We suspect a download might be triggered shortly.
            try:
                # Wait for a download event after navigation failure
                download: Download = page.wait_for_event("download", timeout=3 * 1000)
                final_url = download.url
            except Exception:
                # No download after waiting
                pass

        # 6. Check our results.
        if download_event:
            # A download happened! Use its URL.
            final_url = download_event[0].url
        elif final_url:
            # No download, but navigation succeeded.
            pass # final_url is already set
        else:
            # No download, and navigation failed for an unknown reason.
            # This is where we might see the chrome-error URL.
            if "chrome-error://" in page.url:
                # print(f"Navigation failed. Final page URL: {page.url}")
                final_url = None # Explicitly fail
            else:
                # Still, may be correct URL...
                final_url = page.url

        context.close()
        browser.close()
        
        return final_url.split('?')[0] if final_url else None

def _normalize_attr_value(val: Any) -> str:
    """
    Convert a BeautifulSoup attribute value to a lowercase string safely.
    - If val is None -> ''
    - If it's a list/tuple/AttributeValueList -> join with spaces
    - Otherwise cast to str
    - The return must stay case speciic as some may use this for href and URLs are case sensitive.
    """
    if val is None:
        return ""
    if isinstance(val, (list, tuple)):
        return " ".join(map(str, val))
    return str(val)

def _is_empty_tag(tag: Any) -> bool:
    """
    Return True if tag has no meaningful content:
    - tag.name is empty/None -> consider empty (covers <></>)
    - tag is a void/self-closing tag -> not empty
    - tag has attributes -> consider non-empty (change if desired)
    - any descendant string that is non-whitespace -> not empty
    - any direct child tag that is non-empty -> not empty
    Otherwise -> empty
    """
    # If it's not a Tag (e.g., NavigableString), it's handled elsewhere
    name = getattr(tag, "name", None)
    if not name or str(name).strip() == "":
        # covers the weird case BeautifulSoup produces for "<></>"
        return True

    if name in {"br", "img", "hr", "meta", "link", "input"}:
        return False

    # If attributes exist, treat as non-empty (safe default)
    if bool(tag.attrs):
        return False

    # If any non-whitespace text exists in tag (including descendants), it's not empty
    # But ignore comment nodes
    for s in tag.strings:
        if isinstance(s, Comment):
            continue
        if str(s).strip():
            return False

    # Check direct child tags recursively; if any child is non-empty, this tag is non-empty.
    for child in tag.find_all(recursive=False):
        # skip non-tags
        if getattr(child, "name", None) is None:
            continue
        if not _is_empty_tag(child):
            return False

    # No attributes, no meaningful text, no non-empty children -> empty
    return True

def _remove_empty_tags(soup: BeautifulSoup) -> BeautifulSoup:
    removed_any = True
    while removed_any:
        removed_any = False
        # iterate over a snapshot of all tags (live tree changes while decompose is called)
        for tag in soup.find_all():
            # skip root/soup
            if tag is soup:
                continue
            try:
                if _is_empty_tag(tag) and tag.name != 'a':
                    tag.decompose()
                    removed_any = True
                    break  # restart outer loop because tree changed
            except Exception:
                # be defensive: if anything odd happens, remove the tag
                try:
                    tag.decompose()
                    removed_any = True
                    break
                except Exception:
                    continue
    return soup

REMOVAL_RE = re.compile(
    r"personal data|"
    r"privacy.*policy|your.*privacy|privacy.*notice|terms.*conditions|" 
    r"opt.?out|remove.*list|online.*complaint|"
    r"unsubscribe.*|"
    r"manage.*subscription|email.?preferences|"
    r"update.*preferences|communication.?settings|signup|contact.?us|" \
    r"twitter|facebook|linkedin|youtube|microsoft|"
    r"^mailto:.*|^tel:.*|"
    r"manage.*email|stop.*receiving",
    re.I
)

NOT_REMOVE_LEN_MIN = 10
REMOVAL_LENGTH_MAX = 100

def _remove_irrelevant_urls(soup: BeautifulSoup) -> BeautifulSoup:
    for a in soup.find_all("a"):
        if not getattr(a, "name", None):  # Skip malformed tags like <></>
            continue
        href_val = _normalize_attr_value(a.get("href")).lower()
        link_text = a.get_text(" ", strip=True).lower() if a.get_text() else ""

        # number of words in link text
        if len(re.findall(r"\b\w+\b", link_text)) > NOT_REMOVE_LEN_MIN:
            continue

        # if link_text is None or link_text.replace(" ", "") == '':
        #     a.decompose()
        #     continue

        # --- CASE 1: Anchor itself looks like unsubscribe/preferences ---
        if REMOVAL_RE.search(href_val) or REMOVAL_RE.search(link_text):
            parent = a.parent
            if parent and parent is not soup:
                parent.decompose()
            else:
                a.decompose()
            continue

        # --- CASE 2: Surrounding sentence indicates unsubscribe/preferences ---
        parent = a.parent
        parent_text = parent.get_text(" ", strip=True).lower() if parent else ""
        grandparent = parent.parent if parent else None
        grandparent_text = (
            grandparent.get_text(" ", strip=True).lower()
            if grandparent and grandparent is not soup
            else ""
        )

        if (len(parent_text) < REMOVAL_LENGTH_MAX) and REMOVAL_RE.search(parent_text) or (len(grandparent_text) < REMOVAL_LENGTH_MAX and REMOVAL_RE.search(grandparent_text)):
            if grandparent and grandparent is not soup:
                grandparent.decompose()
            elif parent and parent is not soup:
                parent.decompose()
            continue

    return soup

REMOVE_ATTRS = {
    "id", "class", "target", "bgcolor", "height", "width",
    "align", "cellspacing", "cellpadding", "role",
    "border", "style", "lang",
    "x-bind", "x-data", "x-init",
}

def get_clean_html(html: str) -> CleanHtml:
    # print("Cleaning the HTML...")
    soup = BeautifulSoup(html, 'html.parser')

    # Remove unwanted tags
    for element in soup.find_all(['script', 'meta', 'style', 'base', 'link', 'iframe', 'form', 'input', 'img', 'video', 'svg', 'button', 'footer']):
        element.decompose()

    # Remove comments
    for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
        comment.extract()

    # p_html1 = " ".join(soup.prettify().split())

    # Remove hidden sections
    for element in soup.find_all(attrs={"aria-hidden": "true"}):
        if isinstance(element, Tag):
            if element.name == 'html' or element.name == 'head' or element.name == 'body' or element.name == 'main':
                continue
            element.decompose()

    # p_html2 = " ".join(soup.prettify().split())

    for tag in soup.find_all(True):
        if not isinstance(tag, Tag) or not tag.attrs:
            continue

        # Check style="display: none"
        style = tag.attrs.get("style", "")
        if isinstance(style, str) and "display" in style.lower() and "none" in style.lower():
            tag.decompose()
            continue

        # Check class="hidden"
        classes = tag.attrs.get("class", [])
        if isinstance(classes, list) and "hidden" in classes:
            tag.decompose()
            continue

        # Keep href only on <a>
        if tag.name != "a":
            tag.attrs.pop("href", None)

        # Remove fixed attributes
        for attr in list(tag.attrs):
            if attr in REMOVE_ATTRS:
                del tag.attrs[attr]
            elif attr.startswith("data-") or attr.startswith("aria-"):
                del tag.attrs[attr]

    # p_html3 = " ".join(soup.prettify().split())

    _remove_empty_tags(soup)

    _remove_irrelevant_urls(soup)

    p_html = " ".join(soup.prettify().split())

    anchor_map = {}
    urls_with_specific_text: List[str] = []

    count = 1
    for a_tag in soup.find_all("a", href=True):
        placeholder = f"link/{count}"
        anchor_map[placeholder] = a_tag["href"]
        a_tag["href"] = placeholder
        count += 1

    # After mapping remove all other links
    for tag in soup.find_all():
        for attr, value in list(tag.attrs.items()):
            # Remove only attribute values that start with https://
            if isinstance(value, str) and value.startswith("https://"):
                tag[attr] = ""  # update the soup object itself


    text = soup.get_text()
    lines = (line.strip() for line in text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    p_text = '\n'.join(chunk for chunk in chunks if chunk)

    return CleanHtml(anchor_map=anchor_map, html_anchor_mapped=" ".join(soup.prettify().split()), urls_with_specific_text=urls_with_specific_text, html=p_html, text=p_text )

def dispatch(page: Page, event: dict):
        name: str = event.get("name", "")
        selector: str = event.get("selector", "")

        if name == "navigate":
            page.goto(event["url"], wait_until="domcontentloaded", timeout=5000)
            return;

        if name == "mouse":
            page.mouse.wheel(event["x"],event["y"])
            return;

        if "browserName" in event or selector == "":
            return;

        elif name == "click":
            page.click(
                selector,
                button=event.get("button", "left"),
                click_count=event.get("clickCount", 1),
            )

        elif name == "check":
            page.check(selector)

        elif name == "fill":
            # focus first for robustness
            page.click(selector)
            page.fill(selector, event["text"])

        elif name == "select":
            page.select_option(
                selector,
                value=event["options"],
            )

        elif name == "scroll_to_first":
            page.locator(selector).first.scroll_into_view_if_needed()

        else:
            raise NotImplementedError(f"Unsupported action: {name}")
        
        page.wait_for_timeout(300)

def get_source_page(page: Page, url: str, events: dict | None = None, wait_on_selector: str | None = None, content_selector: str | None = None) -> str | None:
    try:
        # Using 'wait_until="networkidle"' - not good if the page has a video in it that constantly load data.
        # Using domcontentloaded with an extra timeout of 20 seconds. 
        page.goto(url, wait_until="domcontentloaded", timeout=30000)

        if "chrome-error://" in page.url:
            page.reload()
            page.wait_for_timeout(3000)

        # final_html = page.content()

        if wait_on_selector:
            # page.wait_for_function("""
            #         (sel) => {
            #         const el = document.querySelector(sel);
            #         return el && el.innerText.trim().length > 50;
            #         }
            #         """, arg=wait_on_selector, timeout=20000)
            page.locator(wait_on_selector).first.wait_for(
                state="visible",
                timeout=20000
            )

        page.wait_for_timeout(1000)  # final paint

        # final_html = page.content()

        # Run any instruction on the page to open the page content.
        if events:
            for event in events:
                dispatch(page, event)
                page.wait_for_timeout(1000)  # final paint


        page.wait_for_timeout(1000)  # final paint

        if content_selector:
            page.locator(content_selector).first.wait_for(
                state="visible",
                timeout=10000
            )
            page.wait_for_timeout(5000)  # possible loading required based on events
            final_html = page.locator(content_selector).evaluate_all(
                    "els => els.map(e => e.outerHTML).join('\\n')"
                )
        else:
            final_html = page.content()
        
        return final_html

    except Exception as e:
        log.record_notice(f"An unexpected error occurred when trying to get the source page at {url}: {e}")
        # context.close()
        # browser.close()
        return None
        
def get_content(page: Page, initial_url: str) -> Tuple[Optional[str], Optional[bytes]]:
    """
    Resolves redirects using an event listener, which is a cleaner pattern.
    """

    # This list will hold the download object if one is triggered.
    # We use a list because it's a mutable object that the
    # inner 'on_download' function can modify.

    download_event: List[Download] = []

    def on_download(download: Download):
        """Callback function when a download starts."""
        download_event.append(download)

    def save_and_get_data(download: Download) -> bytes:
        temp_path = os.path.join(tempfile.gettempdir(), download.suggested_filename or "download.bin")
        download.save_as(temp_path)
        with open(temp_path, "rb") as f:
            data = f.read()
        os.remove(temp_path)
        return data

    # 1. Register the download event listener *before* navigation.
    page.on("download", on_download)

    html = None
    data = None
    try:
        # 2. Navigate. We wait for 'networkidle' to let JS redirects finish.
        # This call will succeed for HTML pages.
        # It will fail (and throw an error) for downloads,
        # but that's okay!
        page.goto(initial_url, timeout=20000, wait_until="domcontentloaded")

        # 3. Add a manual JS wait / redirect detection loop
        # Some links redirect asynchronously or via window.location.assign after delayed scripts.
        page.wait_for_timeout(5000)

        if "chrome-error://" in page.url:
            page.reload()
            page.wait_for_timeout(3000)

        # 4. If goto() succeeds, the final URL is the page's URL.
        html = page.content()
        
    except Exception as e:
        # 5. If goto() fails (e.g., net::ERR_ABORTED), it's *probably*
        #    because a download was triggered instead of a navigation.
        #    We don't need to do anything here yet, because we'll
        #    check our download_event list.
        #    Sometimes download takes some time to start and set the download_event
        #    We suspect a download might be triggered shortly.
        try:
            # Wait for a download event after navigation failure
            download: Download = page.wait_for_event("download", timeout=3 * 1000)
            data = save_and_get_data(download=download)
        except Exception:
            # No download after waiting
            pass

    # 6. Check our results.
    if download_event:
        # A download happened! Use its URL.
        download = download_event[0]
        data = save_and_get_data(download=download)
    elif html:
        # No download, but navigation succeeded.
        pass # final_url is already set
    else:
        # No download, and navigation failed for an unknown reason.
        # This is where we might see the chrome-error URL.
        if "chrome-error://" in page.url:
            # print(f"Navigation failed. Final page URL: {page.url}")
            html = None # Explicitly fail
        else:
            # Still, may be correct URL...
            html = page.content()
    
    return html, data

MIN_URL_PATH_CHARS = 3
MIN_URL_QUERY_CHARS = 10

def is_not_valid_url(url: str):
    # prevent 'mailto' and 'tel' URLs and home pages
    p_url = urlparse(url)
    if p_url.scheme == 'https' and \
            ((p_url.path and len(p_url.path) > MIN_URL_PATH_CHARS) or (p_url.query and len(p_url.query) > MIN_URL_QUERY_CHARS)) and \
            not REMOVAL_RE.search(url):
        return False
    return True

def is_new(url: str, singular_list: List[str]):
    for item in singular_list:
        if item == url:
            return False
    return True

MAX_AGE_OF_ARTICLE = 60 # days

def get_files_on_page(page: Page, url: str, current_level: int, scrape_levels: int | None = 1, wait_on_selector: str | None = None, content_selector: str | None = None, events: dict | None = None, past_urls: List[str] = []):

    files: List[File] = []
    singular_list = past_urls
    files_failed: List[str] = []

    page_content = get_source_page(page=page, url=url, events=events, wait_on_selector=wait_on_selector, content_selector=content_selector)
    if page_content:
        clean_page = get_clean_html(html=page_content)

    #     if clean_page is not None and clean_page.html_anchor_mapped:
    #         identified_link_codes: List[LinkInfo] = find_links_in_web_page(clean_page.html_anchor_mapped)['list']

    #         # Convert dicts to LinkInfo:
    #         identified_link_codes = [
    #             LinkInfo(**d) if isinstance(d, dict) else d
    #             for d in identified_link_codes
    #         ]

    #         now = datetime.now()
    #         f_url_p = urlparse(url)
    #         for found_link in identified_link_codes:
    #             if found_link.date and datetime.fromisoformat(found_link.date) < now - timedelta(days=MAX_AGE_OF_ARTICLE):
    #                 continue

    #             if not found_link.url.startswith('link/'):
    #                 continue

    #             link_url = clean_page.anchor_map[found_link.url]
    #             p_url_p = urlparse(link_url)
    #             build_url = ""
    #             if not p_url_p.scheme:
    #                 build_url += f"{f_url_p.scheme}:/"
    #             if not p_url_p.hostname:
    #                 build_url += f"/{f_url_p.hostname}"
    #             if build_url:
    #                 link_url = f"{build_url}/{re.sub('^/+', '', link_url)}"
                
    #             if is_not_valid_url(link_url):
    #                 continue

    #             if is_new(link_url, singular_list):
    #                 try:
    #                     date: datetime | None = None
    #                     if found_link.date:
    #                         date = datetime.fromisoformat(found_link.date)
    #                     html_content, download_data = get_content(page, link_url)
    #                     if download_data:
    #                         files.append(File(link_url, 'pdf', download_data, date=date))
    #                         singular_list.append(link_url)
    #                     if html_content:
    #                         if scrape_levels and current_level < scrape_levels:
    #                             files.extend(get_files_on_page(page=page, url=link_url, current_level=current_level + 1, scrape_levels=scrape_levels))
                            
    #                         clean_internal_page = get_clean_html(html=html_content)
    #                         if clean_internal_page is not None:
    #                             files.append(File(link_url, 'html', clean_internal_page.html, date=date))
    #                             singular_list.append(link_url)

    #                 except Exception as e:
    #                     files_failed.append(f"Failed to extract content of URL {link_url}: {e}")

    #     if len(files) == 0 and len(files_failed) != 0:
    #         raise Exception(f"Failed to extract files: {'\n'.join(files_failed)}")
        
    return files
            
def scrape_page(url: str, scrape_levels: int | None = 1, wait_on_selector: str | None = None, content_selector: str | None = None, events: dict | None = None):
    files: List[File] = []
    open_browser: OpenBrowser = OpenBrowser(browser=None, context=None, page=None)

    try:
        with Stealth().use_sync(sync_playwright()) as p:
            open_browser.browser = p.chromium.launch(headless=True,
            # Add extra launch arguments to mimic real Chrome
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-infobars",
                "--disable-web-security",
                "--disable-site-isolation-trials",
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--ignore-certificate-errors"
            ])
            open_browser.context = open_browser.browser.new_context(
                user_agent=REAL_USER_AGENT,
                viewport={'width': 1920, 'height': 1080}
            )      
            open_browser.page = open_browser.context.new_page()

            files = get_files_on_page(page=open_browser.page, url=url, current_level=1, scrape_levels=scrape_levels, wait_on_selector=wait_on_selector, content_selector=content_selector, events=events)
            
            open_browser.context.close()
            open_browser.browser.close()

            return files
        
    except Exception as e:
        raise Exception(f"Failed to extract URLs and subsequent files from webpage content and related links: {e}")


"""
# def get_filename_from_url(url: str) -> str:
#     if not url:
#         return "downloaded_file"
#     path = urlparse(url).path
#     filename = os.path.basename(path)

#     # Provide a default filename if the URL path is empty
#     return filename if filename else path


# def get_content_as_data_block(url: str) -> Optional[bytes]:
#     headers = {
#         'User-Agent': REAL_USER_AGENT,
#         "Accept": "application/pdf,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
#         "Accept-Language": "en-US,en;q=0.9",
#     }
#     try:
#         with requests.get(url, headers=headers, timeout=20, allow_redirects=True) as response:
#             response.raise_for_status()
#             return response.content
#     except requests.exceptions.RequestException as e:
#         log.record_notice(f"Failed with request to get content block of file to download from link {url}: {e}")
#         return None
#     except Exception as e:
#         log.record_notice(f"Failed to get content block of file to download from link {url}: {e}")
#         return None

# def get_html_page(url) -> str | None:
#     headers = {'User-Agent': REAL_USER_AGENT}
#     try:
#         with requests.get(url, headers=headers, timeout=20) as response:
#             response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
#             return response.text
#     except requests.exceptions.RequestException as e:
#         log.record_notice(f"Error fetching URL {url}: {e}")
#         return None
    
# def is_url_html(url: str) -> bool:
#     headers = {'User-Agent': REAL_USER_AGENT}
#     try:
#         # Use a timeout to prevent the request from hanging indefinitely
#         # allow_redirects=True ensures we check the final destination URL
#         response = requests.head(url, headers=headers, timeout=5, allow_redirects=True)
        
#         if response.status_code == 200:
#             # Get the Content-Type header, return an empty string if it doesn't exist
#             content_type = response.headers.get('Content-Type', '')
            
#             # We use 'in' because it might also contain charset info (e.g., 'text/html; charset=utf-8')
#             if 'text/html' in content_type:
#                 return True
                
#     except requests.RequestException as e:
#         # Handle potential connection errors, timeouts, etc.
#         log.record_notice(f"Could not connect to {url}. Error: {e}")
#         return False
        
#     return False

# def get_content_type(url: str) -> str | None:
#     headers = {'User-Agent': REAL_USER_AGENT}
#     try:
#         # Use a timeout to prevent the request from hanging indefinitely
#         # allow_redirects=True ensures we check the final destination URL
#         response = requests.head(url, headers=headers, timeout=5, allow_redirects=True)
        
#         if response.status_code == 200:
#             # Get the Content-Type header, return an empty string if it doesn't exist
#             content_type = response.headers.get('Content-Type', '')
            
#             # We use 'in' because it might also contain charset info (e.g., 'text/html; charset=utf-8')
#             if 'text/html' in content_type:
#                 return 'html' 
#             if 'application/pdf' in content_type:
#                 return 'pdf'   
#     except requests.RequestException as e:
#         # Handle potential connection errors, timeouts, etc.
#         log.record_notice(f"Could not connect to {url}. Error: {e}")
#         return None
        
#     return None

# def save_content_block(content_block: bytes, filename: str):
#     try:
#         # Decide how to write based on the file extension, not the data type.
#         if filename.lower().endswith(('.html', '.htm')):
#             # Decode bytes into a string only when writing a text file.
#             print("Detected HTML page. Writing in text mode...")
#             text_content = content_block.decode('utf-8')
#             with open(filename, 'w', encoding='utf-8') as f:
#                 f.write(text_content)
#         elif filename.lower().endswith(('.pdf')):
#             # Write the raw bytes directly for binary files.
#             print("Detected PDF file. Writing in binary mode...")
#             with open(filename, 'wb') as f:
#                 f.write(content_block)
#         else:
#             print(f"File not saved - not in permitted format.")
#     except UnicodeDecodeError:
#         print(f"Warning: Could not decode content as UTF-8 for '{filename}'. Saving as raw binary instead.")
#         with open(filename, 'wb') as f:
#             f.write(content_block)
#     except IOError as e:
#         print(f"Failed to save file: {e}")

"""