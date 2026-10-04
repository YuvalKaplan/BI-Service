"""
Checks whether EDGAR accepts requests from the machine it runs on, to tell a User-Agent problem
from a refusal of the machine itself - the SEC answers both with the same 403 page ("Your Request
Originates from an Undeclared Automated Tool"). Run it where the cron gets the 403 (Render) and
on the desktop, which EDGAR accepts, and compare.

  1. SECRET_SEC_USER_AGENT as this process sees it: length, leading / trailing whitespace, quotes,
     and any character outside printable ASCII (an invisible one pasted with the value) - the
     email's user part masked. Python and OpenSSL versions (the TLS client the SEC sees).
  2. One request each, a few seconds apart, for the form index the cron reads first:
       clean   - "<name> <email>" rebuilt in plain ASCII from the variable
       as set  - the variable exactly as set (what the cron sends)
       curl    - the variable through curl, another HTTP / TLS client
     each with its status and, on a 403, the heading of the SEC's page; then what they point to.

No database. Usage (from the project root; on Render as the cron job's command, then restore it):
    python -m scripts.debug_sec_access
"""
import os
import platform
import re
import shutil
import ssl
import subprocess
import sys
import tempfile
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from dotenv import load_dotenv
from modules.sec import edgar

load_dotenv()

URL = f"{edgar.EDGAR_ARCHIVES_URL}/full-index/2025/QTR4/form.idx"
PAUSE = 3  # seconds between requests - far below the SEC's rate limit
EMAIL = re.compile(r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}')


def describe(ua: str) -> list[str]:
    masked = EMAIL.sub(lambda m: '***@' + m.group().split('@', 1)[1], ua)
    lines = [f"value:  {masked!r}", f"length: {len(ua)}"]
    if ua != ua.strip():
        lines.append("WARNING: leading / trailing whitespace")
    if ua[0] in '"\'' or ua[-1] in '"\'':
        lines.append("WARNING: quotes around the value")
    odd = [f"U+{ord(c):04X} at {i}" for i, c in enumerate(ua) if not ' ' <= c <= '~']
    if odd:
        lines.append(f"WARNING: characters outside printable ASCII: {', '.join(odd)}")
    if not EMAIL.search(ua):
        lines.append("WARNING: no email address")
    return lines


def python_request(ua: str) -> str:
    """As edgar.get sends it."""
    try:
        with urlopen(Request(URL, headers={'User-Agent': ua, 'Accept-Encoding': 'gzip'}), timeout=edgar.REQUEST_TIMEOUT) as response:
            return str(response.status)
    except HTTPError as e:
        return f"{e.code} {edgar.error_page_heading(e)}"
    except Exception as e:
        return f"failed: {e}"


def curl_request(ua: str) -> str:
    curl = shutil.which('curl')
    if not curl:
        return "curl not available"
    with tempfile.TemporaryDirectory() as folder:
        page_path = os.path.join(folder, 'page')
        result = subprocess.run([curl, '-s', '--compressed', '--max-time', str(edgar.REQUEST_TIMEOUT), '-A', ua,
                                 '-o', page_path, '-w', '%{http_code}', URL], capture_output=True, text=True)
        status = result.stdout.strip()
        if status != '403':
            return status if status and status != '000' else f"failed: {result.stderr.strip() or result.returncode}"
        with open(page_path, encoding='utf-8', errors='replace') as f:
            return f"403 {edgar.page_heading(f.read())}"


def verdict(results: dict[str, str]) -> str:
    ok = {label: status == '200' for label, status in results.items() if not status.startswith('curl not')}
    if all(ok.values()):
        return "EDGAR accepts requests from this machine (a refusal seen earlier here was temporary)."
    if ok.get('clean') and not ok.get('as set'):
        return "The variable's value is what the SEC refuses (see the warnings above) - the clean rebuild gets through."
    if not ok.get('clean', ok.get('as set')) and ok.get('curl'):
        return "The SEC refuses Python's HTTP client from this machine, curl gets through - its TLS client, not the User-Agent."
    if not any(ok.values()):
        return "The SEC refuses this machine whatever the User-Agent or client - its IP."
    return "Mixed results - compare with a run on the desktop."


if __name__ == '__main__':
    ua = os.getenv('SECRET_SEC_USER_AGENT')
    print(f"Python {platform.python_version()}, {ssl.OPENSSL_VERSION}")
    print("SECRET_SEC_USER_AGENT")
    if not ua:
        print("  not set")
        sys.exit(1)
    for line in describe(ua):
        print(f"  {line}")

    checks = []
    match = EMAIL.search(ua)
    if match:
        name = re.sub(r'[^ -~]', '', ua[:match.start()]).strip(' "\'') or 'BI-Service'
        checks.append(('clean', lambda: python_request(f"{name} {match.group()}")))
    checks += [('as set', lambda: python_request(ua)), ('curl', lambda: curl_request(ua))]

    print(f"\nEDGAR {URL}")
    results = {}
    for n, (label, check) in enumerate(checks):
        if n:
            time.sleep(PAUSE)
        results[label] = check()
        print(f"  {label:7} {results[label]}")
    print(f"\n{verdict(results)}")
