import time
from curl_cffi import requests as cffi_requests
from curl_cffi.requests.exceptions import RequestException as CffiRequestException

BASE_URL = "https://www.nb.com"
FP_AUDIENCE_ID = "{84F10838-2ADF-40AC-98BD-AF64F5ED72CF}"
HOMEPAGE_CONTEXT_ID = "{2CBF6AC0-C426-4AA8-AE21-CF7EF3CE3299}"
NBCR_FUND_CODE = "4023"

# curl error 23 (write-callback failure) is transient with Akamai; retry across
# multiple Chrome fingerprints before giving up.
_IMPERSONATE_VERSIONS = ["chrome124", "chrome120", "chrome131", "chrome110"]
_MAX_RETRIES = 3


def _get_with_retry(session: cffi_requests.Session, *args, **kwargs):
    """Retry a GET up to _MAX_RETRIES times with exponential backoff."""
    for attempt in range(_MAX_RETRIES):
        try:
            return session.get(*args, **kwargs)
        except CffiRequestException:
            if attempt == _MAX_RETRIES - 1:
                raise
            time.sleep(2 ** attempt)
    raise RuntimeError("_MAX_RETRIES must be > 0")


def download_nb_etf_holdings(output_path: str = "NBCR_Core_Equity_ETF_Holdings.xls") -> str:
    exc: Exception = RuntimeError("No impersonation versions available")
    for version in _IMPERSONATE_VERSIONS:
        try:
            return _download(output_path, impersonate=version)
        except CffiRequestException as e:
            print(f"  [retry] {version} failed: {e}")
            exc = e
    raise exc


def _download(output_path: str, impersonate: str) -> str:
    print(f"Using impersonation: {impersonate}")
    session = cffi_requests.Session(impersonate=impersonate)  # type: ignore[call-arg]

    # ── Step 1: Visit homepage ──
    print("Step 1: Loading homepage...")
    resp = _get_with_retry(session, f"{BASE_URL}/", timeout=30)
    resp.raise_for_status()
    print(f"  → {resp.url}  [{resp.status_code}]")

    # ── Step 2: GetAudiences (initialises audience state) ──
    print("Step 2: Fetching audience configuration...")
    resp = session.post(
        f"{BASE_URL}/api/Sitecore/Audience/GetAudiences",
        headers={
            "X-Requested-With": "XMLHttpRequest",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Referer": f"{BASE_URL}/en/global/home",
        },
        timeout=30,
    )
    resp.raise_for_status()
    audience_data = resp.json()
    print(f"  → Current region: {audience_data.get('audienceObj', {}).get('currRegion')}")

    # ── Step 3: AcceptTerms — select US + Financial Professionals ──
    print("Step 3: Selecting United States / Financial Professionals...")
    resp = session.post(
        f"{BASE_URL}/api/Sitecore/Audience/AcceptTerms",
        headers={
            "X-Requested-With": "XMLHttpRequest",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Referer": f"{BASE_URL}/en/global/home",
        },
        data={
            "id": FP_AUDIENCE_ID,
            "currentPage": HOMEPAGE_CONTEXT_ID,
            "language": "en",
            "redirect": "false",
            "url": f"{BASE_URL}/en/global/home",
        },
        timeout=30,
    )
    resp.raise_for_status()
    accept_data = resp.json()
    redirect_url = BASE_URL + accept_data["redirectUrl"]
    print(f"  → Redirecting to: {redirect_url}")

    # ── Step 4: Follow redirect to US Financial Professionals page ──
    print("Step 4: Loading US Financial Professionals page...")
    resp = _get_with_retry(session, redirect_url, timeout=30)
    resp.raise_for_status()
    print(f"  → {resp.url}  [{resp.status_code}]")

    # ── Step 5: Navigate to the Core Equity ETF fund page ──
    fund_url = f"{BASE_URL}/en/us/products/etfs/core-equity-etf"
    print("Step 5: Loading Core Equity ETF fund page...")
    resp = _get_with_retry(session, fund_url, timeout=30)
    resp.raise_for_status()
    print(f"  → {resp.url}  [{resp.status_code}]")
    if "DownloadETFDetailedHoldingsXLS" in resp.text:
        print("  ✓ Holdings download link confirmed on page")

    # ── Step 6: Download the holdings XLS file ──
    print("Step 6: Downloading holdings file...")
    resp = _get_with_retry(
        session,
        f"{BASE_URL}/api/Sitecore/Product/DownloadETFDetailedHoldingsXLS",
        params={"nbmicode": NBCR_FUND_CODE},
        headers={"Referer": fund_url},
        timeout=60,
    )
    resp.raise_for_status()

    content_type = resp.headers.get("Content-Type", "")
    print(f"  Content-Type : {content_type}")
    print(f"  File size    : {len(resp.content):,} bytes")

    if "excel" not in content_type.lower() and "spreadsheet" not in content_type.lower():
        raise ValueError(f"Unexpected content type: {content_type}")

    with open(output_path, "wb") as f:
        f.write(resp.content)

    print(f"\n✅ Saved to: {output_path}")
    return output_path


if __name__ == "__main__":
    download_nb_etf_holdings()