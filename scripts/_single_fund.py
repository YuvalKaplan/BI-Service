import asyncio
from pathlib import Path
from playwright.async_api import async_playwright, Download

BASE_URL = "https://www.nb.com"
FUND_URL = f"{BASE_URL}/en/us/products/etfs/core-equity-etf"
OUTPUT_PATH = "NBCR_Core_Equity_ETF_Holdings.xls"


async def download_nb_etf_holdings(output_path: str = OUTPUT_PATH) -> str:

    async with async_playwright() as p:

        browser = await p.chromium.launch(headless=False)  # set headless=False to watch it run
        context = await browser.new_context(
            accept_downloads=True,
            locale="en-US",
            viewport={"width": 1280, "height": 800},
        )
        page = await context.new_page()

        # ── Step 1: Load homepage ──
        print("Step 1: Loading homepage...")
        await page.goto(f"{BASE_URL}/", wait_until="domcontentloaded", timeout=60_000)
        print(f"  → {page.url}")

        # ── Step 2: Handle cookie consent banner (if present) ──
        print("Step 2: Handling cookie consent...")
        try:
            accept_btn = page.get_by_role("button", name="ACCEPT ALL")
            await accept_btn.wait_for(state="visible", timeout=8_000)
            await accept_btn.click()
            print("  → Cookie banner accepted")
        except Exception:
            print("  → No cookie banner found, continuing")

        # ── Step 3: Handle the "not available for global audience" modal (if present) ──
        print("Step 3: Checking for global audience modal...")
        try:
            close_btn = page.locator("button.close-modal")
            await close_btn.wait_for(state="visible", timeout=5_000)
            await close_btn.click()
            print("  → Closed global audience modal")
        except Exception:
            print("  → No modal found, continuing")

        # ── Step 4: Open the location selector panel ──
        print("Step 4: Opening location selector...")
        # Click the globe icon / "SELECT YOUR LOCATION" toggle
        await page.locator("#toggle-shelf").first.click()
        # Wait for the panel to expand
        await page.locator(".audience-selector-container").wait_for(state="visible", timeout=8_000)
        print("  → Location panel opened")

        # ── Step 5: Select United States ──
        print("Step 5: Selecting United States...")
        await page.locator("[data-location-id='us']").click()
        print("  → United States selected")

        # ── Step 6: Select Financial Professionals ──
        print("Step 6: Selecting Financial Professionals...")
        await page.locator("[data-audience-id='84f10838-2adf-40ac-98bd-af64f5ed72cf']").click()
        print("  → Financial Professionals selected")

        # ── Step 7: Click Submit ──
        print("Step 7: Submitting audience selection...")
        async with page.expect_navigation(timeout=15_000):
            await page.locator(".audience-selector-submit").click()
        print(f"  → Navigated to: {page.url}")

        # ── Step 8: Navigate to the Core Equity ETF fund page ──
        print("Step 8: Loading Core Equity ETF fund page...")
        await page.goto(FUND_URL, wait_until="domcontentloaded", timeout=30_000)
        print(f"  → {page.url}")

        # Verify the holdings link is present
        holdings_link = page.locator("a[href*='DownloadETFDetailedHoldingsXLS']")
        await holdings_link.wait_for(state="visible", timeout=10_000)
        print("  ✓ Holdings download link found")

        # ── Step 9: Download the holdings file ──
        print("Step 9: Downloading holdings file...")
        async with page.expect_download(timeout=30_000) as download_info:
            await holdings_link.click()

        download: Download = await download_info.value
        await download.save_as(output_path)

        size = Path(output_path).stat().st_size
        print(f"  Content suggestion: {download.suggested_filename}")
        print(f"  File size         : {size:,} bytes")

        await browser.close()

    print(f"\n✅ Saved to: {Path(output_path).resolve()}")
    return output_path


if __name__ == "__main__":
    asyncio.run(download_nb_etf_holdings())