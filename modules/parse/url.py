import os
import log
import re
import tempfile
import time
from datetime import date
from typing import List
from dataclasses import dataclass
from playwright.sync_api import sync_playwright, Download, Browser, BrowserContext, Page
from playwright_stealth import Stealth
from modules.core.util import clean_date
from modules.object.provider import Provider, Mapping, getMappingFromJson
from modules.object.provider_etf import EtfDownload, ProviderEtf, fetch_by_provider_id
from modules.object.categorize_etf import CategorizeEtfDownload
from modules.core.protocols import CategorizeEtfProtocol



ENV_TYPE = os.environ.get("ENV_TYPE")

SCRAPE_MAX_RETRIES = 3
SCRAPE_RETRY_DELAY_SECONDS = 3

# Add extra launch arguments to mimic real Chrome
CHROME_LAUNCH_ARGS = [
        "--disable-blink-features=AutomationControlled",
        "--disable-infobars",
        "--disable-dev-shm-usage",
        "--no-sandbox"
        #     "--disable-web-security",
        #     "--ignore-certificate-errors"
        #     "--disable-site-isolation-trials",
    ]

# A common, modern user-agent string
REAL_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

@dataclass
class OpenBrowser:
    browser: Browser | None
    context: BrowserContext | None
    page: Page | None

def get_date_on_page(page: Page, mapping: Mapping) -> date | None:
    try:
        on_page = mapping.date.on_page
        if not on_page or not on_page.location:
            return None

        locator = page.locator(on_page.location).first
        locator.wait_for(state="visible", timeout=15000)

        raw_text = locator.inner_text()
       
        # Normalize whitespace (handles line breaks between label and date)
        normalized = " ".join(raw_text.split())
        
        if on_page.text_before:
            # Case 1: anchor text provided
            anchor = on_page.text_before.strip()
            if anchor not in normalized:
                return None
            
            candidate = normalized.split(anchor, 1)[1].strip()
            return clean_date(candidate, mapping.date.format).date()
            
        else:
            # Case 2: one date in section
            return clean_date(normalized, mapping.date.format)
            
        return None

    except Exception as e:
        log.record_notice(f"An unexpected error occurred when trying to get the date on the page: {e}")
        return None


def dispatch(page: Page, event: dict) -> None:
        action_timout = 7000
        name: str = event.get("name", "")
        selector: str = event.get("selector", "")

        if name == "navigate":
            page.goto(event["url"], wait_until="domcontentloaded", timeout=action_timout)
            return;

        if name == "mouse":
            page.mouse.wheel(event["x"],event["y"])
            return;

        if "browserName" in event or selector == "":
            return;

        elif name == "click":
            page.click(selector, button=event.get("button", "left"), click_count=event.get("clickCount", 1), timeout=action_timout, force=True)

        elif name == "check":
            page.check(selector, timeout=action_timout)

        elif name == "fill":
            # focus first for robustness
            page.click(selector, timeout=action_timout)
            page.fill(selector, event["text"], timeout=action_timout)

        elif name == "select":
            page.select_option(selector, value=event["options"], timeout=action_timout)

        elif name == "scroll_to_first":
            page.locator(selector).first.scroll_into_view_if_needed(timeout=action_timout)

        else:
            raise NotImplementedError(f"Unsupported action: {name}")
        
        page.wait_for_timeout(1000)

def save_and_get_data(download: Download) -> bytes:
        temp_path = os.path.join(tempfile.gettempdir(), download.suggested_filename or "download.bin")
        download.save_as(temp_path)
        with open(temp_path, "rb") as f:
            data = f.read()
        os.remove(temp_path)
        return data

def get_holdings(page: Page, trigger_download: dict) -> tuple[str | None, bytes | None, str | None]:
    try:
        page.wait_for_timeout(2000)
        page.locator(trigger_download['selector']).first.wait_for(
            state="visible",
            timeout=15000
        )
        dispatch(page, { 'name': 'scroll_to_first', 'selector': trigger_download['selector'] })
        page.wait_for_timeout(2000)

        # # Perform the click
        with page.expect_download() as download_info:
            # Perform the action that initiates download
            dispatch(page, { 'name': 'click', 'selector': trigger_download['selector'] })

        download = download_info.value
        return download.suggested_filename, save_and_get_data(download=download), None

        # download.save_as(download.suggested_filename)

    except Exception as e:
        return None, None, f"An unexpected error occurred when trying to get the holdings:\n {e}"


def open_page(page: Page, url: str, wait_pre_events: str | None, wait_post_events: str | None, events: dict | None) -> bool:
    try:

        # Using 'wait_until="networkidle"' - not good if the page has a video in it that constantly load data.
        # Using domcontentloaded with an extra timeout of 20 seconds. 
        page.goto(url, wait_until="domcontentloaded", timeout=30000)

        if "chrome-error://" in page.url:
            page.reload()
            page.wait_for_timeout(3000)

        if wait_pre_events:
            page.locator(wait_pre_events).first.wait_for(
                state="visible",
                timeout=30000
            )

        page.wait_for_timeout(1000)  # final paint

        # Run any instruction on the page to open the page content.
        if events:
            for event in events:
                max_retries = 2
                for attempt in range(max_retries):
                    try:
                        dispatch(page, event)
                        page.wait_for_timeout(2000)  # final paint
                        break
                    except Exception as e:
                        if attempt == max_retries - 1:
                            raise
                        log.record_notice(f"Dispatch failed on attempt {attempt + 1}, retrying... Error: {e}")
                        page.wait_for_timeout(1000)


        page.wait_for_timeout(2000)  # final paint

        if wait_post_events:
            page.locator(wait_post_events).first.wait_for(
                state="visible",
                timeout=10000
            )

        return True
 
    except Exception as e:
        log.record_notice(f"An unexpected error occurred when trying to get the source page at {url}: {e}")
        return False


def scrape_provider(cp: Provider) -> List[EtfDownload]:
    if cp.id is None or cp.url_start is None:
        raise Exception('Missing URL for provider.')

    etf_list = fetch_by_provider_id(cp.id)
    log.record_status(f"Scraping {len(etf_list)} ETFs from provider '{cp.name}'")

    downloads: List[EtfDownload] = []

    with Stealth().use_sync(sync_playwright()) as p:
        browser = p.chromium.launch(headless=True, args=CHROME_LAUNCH_ARGS)
        context = browser.new_context(
            user_agent=REAL_USER_AGENT,
            viewport={'width': 1920, 'height': 1080},
            accept_downloads=True
        )
        page = context.new_page()

        if not open_page(page=page, url=cp.url_start, wait_pre_events=cp.wait_pre_events, wait_post_events=cp.wait_post_events, events=cp.events):
            raise Exception(f"Failed to open provider start URL: {cp.url_start}")

        for etf in etf_list:
            last_error = None
            for attempt in range(SCRAPE_MAX_RETRIES):
                try:
                    trigger_download = etf.trigger_download or cp.trigger_download
                    if etf.id is None or etf.url is None or trigger_download is None:
                        raise Exception('Missing URL or trigger_download for provider ETF.')

                    log.record_status(f"Opening ETF '{etf.name}' - [{etf.id}] ('{cp.name}' - [{cp.id}]) for scraping")
                    if not open_page(page=page, url=etf.url, wait_pre_events=etf.wait_pre_events, wait_post_events=etf.wait_post_events, events=etf.events):
                        raise Exception(f"Failed to open ETF URL: {etf.url}")

                    found_date_from_page = None
                    mapping = etf.mapping or cp.mapping
                    if mapping:
                        map = getMappingFromJson(mapping)
                        if map.date.on_page:
                            found_date_from_page = get_date_on_page(page=page, mapping=map)
                            if not found_date_from_page:
                                raise Exception('ETF holdings date from page could not be confirmed.')

                    file_name, data, error = get_holdings(page=page, trigger_download=trigger_download)
                    if error:
                        raise Exception(error)

                    downloads.append(EtfDownload(provider=cp, etf=etf, file_name=file_name, data=data, date_from_page=found_date_from_page))
                    break

                except Exception as e:
                    last_error = e
                    if attempt < SCRAPE_MAX_RETRIES - 1:
                        log.record_notice(f"ETF '{etf.name}' - [{etf.id}] attempt {attempt + 1}/{SCRAPE_MAX_RETRIES} failed, retrying in {(attempt + 1) * SCRAPE_RETRY_DELAY_SECONDS}s... Error: {e}")
                        time.sleep((attempt + 1) * SCRAPE_RETRY_DELAY_SECONDS)
                    else:
                        log.record_error(f"Failed to scrape ETF '{etf.name}' - [{etf.id}] ('{cp.name}' - [{cp.id}]) after {SCRAPE_MAX_RETRIES} attempts: {last_error}")

        context.close()
        browser.close()

    return downloads


def scrape_provider_etf(cp: Provider, etf: ProviderEtf) -> EtfDownload:
    if cp.id is None or cp.url_start is None:
        raise Exception('Missing URL for provider.')
    trigger_download = etf.trigger_download or cp.trigger_download
    if etf.id is None or etf.url is None or trigger_download is None:
        raise Exception('Missing URL or trigger_download for provider ETF.')

    with Stealth().use_sync(sync_playwright()) as p:
        browser = p.chromium.launch(headless=False, args=CHROME_LAUNCH_ARGS)
        context = browser.new_context(
            user_agent=REAL_USER_AGENT,
            viewport={'width': 1920, 'height': 1080},
            accept_downloads=True
        )
        page = context.new_page()

        if not open_page(page=page, url=cp.url_start, wait_pre_events=cp.wait_pre_events, wait_post_events=cp.wait_post_events, events=cp.events):
            raise Exception(f"Failed to open provider start URL: {cp.url_start}")

        log.record_status(f"Opening ETF '{etf.name}' - [{etf.id}] ('{cp.name}' - [{cp.id}]) for scraping")
        if not open_page(page=page, url=etf.url, wait_pre_events=etf.wait_pre_events, wait_post_events=etf.wait_post_events, events=etf.events):
            raise Exception(f"Failed to open ETF URL: {etf.url}")

        found_date_from_page = None
        mapping = etf.mapping or cp.mapping
        if mapping:
            map = getMappingFromJson(mapping)
            if map.date.on_page:
                found_date_from_page = get_date_on_page(page=page, mapping=map)
                if not found_date_from_page:
                    raise Exception('ETF holdings date from page could not be confirmed.')

        file_name, data, error = get_holdings(page=page, trigger_download=trigger_download)
        context.close()
        browser.close()

    if error:
        raise Exception(f"Failed to download holdings: {error}")

    return EtfDownload(provider=cp, etf=etf, file_name=file_name, data=data, date_from_page=found_date_from_page)


def scrape_categorizer(etf: CategorizeEtfProtocol) -> CategorizeEtfDownload:
    last_error = None
    for attempt in range(SCRAPE_MAX_RETRIES):
        try:
            download: CategorizeEtfDownload = CategorizeEtfDownload(etf=etf)
            open_browser: OpenBrowser = OpenBrowser(browser=None, context=None, page=None)

            with Stealth().use_sync(sync_playwright()) as p:
                open_browser.browser = p.chromium.launch(headless=True, args=CHROME_LAUNCH_ARGS)
                open_browser.context = open_browser.browser.new_context(
                    user_agent=REAL_USER_AGENT,
                    viewport={'width': 1920, 'height': 1080},
                    accept_downloads=True
                )
                open_browser.page = open_browser.context.new_page()

                if etf.id == None or etf.url == None or etf.trigger_download == None:
                    raise Exception('Missing URL or Trigger Method for categorizer etf.')

                if open_page(page=open_browser.page, url=etf.url, wait_pre_events=etf.wait_pre_events, wait_post_events=etf.wait_post_events, events=etf.events):
                    file_name, data, error = get_holdings(page=open_browser.page, trigger_download=etf.trigger_download)
                    if file_name and data:
                        download.file_name = file_name
                        download.data = data
                    if error:
                        raise Exception(error)

                open_browser.context.close()
                open_browser.browser.close()

                return download

        except Exception as e:
            last_error = e
            if attempt < SCRAPE_MAX_RETRIES - 1:
                log.record_notice(f"scrape_categorizer attempt {attempt + 1}/{SCRAPE_MAX_RETRIES} failed, retrying in {(attempt + 1) * SCRAPE_RETRY_DELAY_SECONDS}s... Error: {e}")
                time.sleep((attempt + 1) * SCRAPE_RETRY_DELAY_SECONDS)

    raise Exception(f"Failed to extract URLs and subsequent files from categorizer ETF links after {SCRAPE_MAX_RETRIES} attempts: {last_error}")

