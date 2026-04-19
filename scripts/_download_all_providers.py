import atexit
import os
import shutil
from modules.object.exit import cleanup
from modules.object import provider
from modules.parse.url import scrape_provider

atexit.register(cleanup)

DOWNLOADS_DIR = os.path.join(os.path.dirname(__file__), '..', '.downloads')

if __name__ == '__main__':
    if os.path.exists(DOWNLOADS_DIR):
        shutil.rmtree(DOWNLOADS_DIR)
    os.makedirs(DOWNLOADS_DIR)

    providers = provider.fetch_active_providers()
    print(f"Found {len(providers)} active providers.")

    for p in providers:
        try:
            print(f"Scraping provider [{p.id}] - {p.name}...")
            downloads = scrape_provider(p)

            if not downloads:
                print(f"  No downloads returned for [{p.id}] - {p.name}.")
                continue

            provider_dir = os.path.join(DOWNLOADS_DIR, f"{p.id} - {p.name}")
            os.makedirs(provider_dir, exist_ok=True)

            for d in downloads:
                if not d.file_name or not d.data:
                    print(f"  Skipping empty download for ETF '{d.etf.name}'.")
                    continue
                file_name = f"{d.etf.region} - {d.file_name}" if d.etf.region else d.file_name
                out_path = os.path.join(provider_dir, file_name)
                with open(out_path, 'wb') as f:
                    f.write(d.data)
                print(f"  Saved: {out_path}")

        except Exception as e:
            print(f"  Error scraping provider [{p.id}] - {p.name}: {e}")
