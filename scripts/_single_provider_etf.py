import atexit
import os
from modules.object.exit import cleanup
from modules.object import provider, provider_etf
from modules.parse.url import scrape_provider_etf

atexit.register(cleanup)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '..', '.downloads')

if __name__ == '__main__':
    try:
        provider_etf_id = 156
        etf = provider_etf.fetch_by_id(provider_etf_id)
        if not etf or not etf.provider_id:
            raise Exception(f"No provider_etf found with id={provider_etf_id}")

        p = provider.fetch_by_id(etf.provider_id)
        if not p:
            raise Exception(f"No provider found with id={etf.provider_id}")

        d = scrape_provider_etf(p, etf)

        if not d.file_name or not d.data:
            raise Exception("Download returned no file.")
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        out_path = os.path.join(OUTPUT_DIR, d.file_name)
        with open(out_path, 'wb') as f:
            f.write(d.data)
        print(f"Saved: {out_path}")

    except Exception as e:
        print(f"Error in scraping single provider ETF: {e}")
