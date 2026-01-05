import os
import log
import re
import tempfile
from datetime import datetime
from typing import List
from dataclasses import dataclass
from playwright.sync_api import sync_playwright, Download, Browser, BrowserContext, Page
from playwright_stealth import Stealth
from modules.object.provider import Provider 
from modules.object.provider_etf import EtfDownload, fetch_by_provider_id

ENV_TYPE = os.environ.get("ENV_TYPE")

# A common, modern user-agent string
REAL_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

@dataclass
class OpenBrowser:
    browser: Browser | None
    context: BrowserContext | None
    page: Page | None

def dispatch(page: Page, event: dict):
        action_timout = 5000
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
        
        page.wait_for_timeout(750)

def save_and_get_data(download: Download) -> bytes:
        temp_path = os.path.join(tempfile.gettempdir(), download.suggested_filename or "download.bin")
        download.save_as(temp_path)
        with open(temp_path, "rb") as f:
            data = f.read()
        os.remove(temp_path)
        return data

def get_holdings(page: Page, trigger_download: dict) -> tuple[str | None, bytes | None]:
    try:
        page.wait_for_timeout(1500)
        page.locator(trigger_download['selector']).first.wait_for(
            state="visible",
            timeout=15000
        )
        dispatch(page, { 'name': 'scroll_to_first', 'selector': trigger_download['selector'] })
        page.wait_for_timeout(1000)

        # # Perform the click
        with page.expect_download() as download_info:
            # Perform the action that initiates download
            dispatch(page, { 'name': 'click', 'selector': trigger_download['selector'] })

        download = download_info.value
        return download.suggested_filename, save_and_get_data(download=download)

        # download.save_as(download.suggested_filename)

    except Exception as e:
        log.record_notice(f"An unexpected error occurred when trying to get the holdings: {e}")
        return None, None


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
                timeout=20000
            )

        page.wait_for_timeout(1000)  # final paint

        # Run any instruction on the page to open the page content.
        if events:
            for event in events:
                dispatch(page, event)
                page.wait_for_timeout(1000)  # final paint


        page.wait_for_timeout(1000)  # final paint

        if wait_post_events:
            page.locator(wait_post_events).first.wait_for(
                state="visible",
                timeout=10000
            )

        return True
 
    except Exception as e:
        log.record_notice(f"An unexpected error occurred when trying to get the source page at {url}: {e}")
        return False


def scrape_provider(cp: Provider):
    downloads: List[EtfDownload] = []
    open_browser: OpenBrowser = OpenBrowser(browser=None, context=None, page=None)

    try:
        with Stealth().use_sync(sync_playwright()) as p:
            open_browser.browser = p.chromium.launch(headless=False,
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
                viewport={'width': 1920, 'height': 1080},
                accept_downloads=True
            )      
            open_browser.page = open_browser.context.new_page()

            if cp.id == None or cp.url_start == None:
                raise Exception('Missing URL for provider.')
            
            if cp.id and open_page(page=open_browser.page, url=cp.url_start, wait_pre_events=cp.wait_pre_events, wait_post_events=cp.wait_post_events, events=cp.events):
                etf_list = fetch_by_provider_id(cp.id)
                for etf in etf_list:
                    trigger_download = etf.trigger_download or cp.trigger_download

                    if etf.id == None or etf.url == None or trigger_download == None:
                        raise Exception('Missing URL or Trigger Method for provider etf.')
                    
                    if open_page(page=open_browser.page, url=etf.url, wait_pre_events=etf.wait_pre_events, wait_post_events=etf.wait_post_events, events=etf.events):
                        file_name, data = get_holdings(page=open_browser.page, trigger_download=trigger_download)
                        if file_name and data:
                            downloads.append(EtfDownload(provider=cp, etf=etf, file_name=file_name, data=data))

            open_browser.context.close()
            open_browser.browser.close()

            return downloads
        
    except Exception as e:
        raise Exception(f"Failed to extract URLs and subsequent files from webpage content and related links: {e}")

