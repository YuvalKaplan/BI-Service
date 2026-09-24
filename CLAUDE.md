# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Activate virtual environment (Windows)
.venv/Scripts/activate

# Run live cron pipeline
python service_cron.py

# Run backtesting
python modules/bt/run.py

# Run individual scripts (ad-hoc / debugging)
python scripts/current_categorize_tickers.py
python scripts/single_provider.py

# Update requirements
pip freeze > requirements.txt
```

No automated test suite exists. Validation is done by running the cron or BT pipelines directly.

### Dev/Prod DB switching

`service_cron.py` and every script in `scripts/` default to the **development** database (`db_pool_instance`, live pipeline only). Pass `--prod` to point that same pool at production instead, or `--dev` to force development even if `ENV_TYPE=production` is set in the environment:

```bash
python service_cron.py --prod
python scripts/single_provider.py --prod
```

The resolved environment is printed at startup (`[db] Resolved database environment: ...`) and logged via `log.record_status`. This flag only affects the live pool — `db_pool_instance_bt` (backtesting) always uses the development database regardless.

Pass `--headed` to run Playwright with a visible browser window instead of headless (useful together with `--prod` for debugging scraping issues against production data):

```bash
python scripts/single_provider.py --prod --headed
```

## Architecture

**Two separate pipelines share a codebase:**

- **Live** (`service_cron.py` → `modules/cron/`): scheduled weekly data ingestion and analysis
- **Backtesting** (`modules/bt/run.py` → `modules/bt/`): historical simulation over a date range

Each pipeline has its own PostgreSQL database (`best_ideas` and `best_ideas_bt`) and its own set of object modules with independent dataclasses and DB connections.

### Live Pipeline Flow

```
etf_downloader       → scrapes provider websites (Playwright) → provider_etf_holding
stocks_downloader    → FinancialModelingPrep API              → ticker, ticker_value
stocks_categorize    → scrapes style/ESG ETFs                 → ticker (style_type, esg_qualified)
benchmark_generator  → FMP screener API                       → benchmark_holding
best_ideas_generator → active weight algorithm                → best_idea (self + full_universe modes)
funds_update         → fund strategy composition              → fund_holding
```

**Cron schedule** (weekday 0=Monday):
- Tue–Sat: ETF holdings download, stock data download, categorization, ticker profile refresh
- Sun: Categorization ETFs, ESG update
- Wed: Master ticker sync (`modules/ticker/master.py`), accumulated market cap refresh, benchmark blend holdings refresh (`benchmark_generator.run()`), best ideas generation, fund updates

Benchmark generation runs on Wednesday (moved from Sunday) so it's built the same day best ideas/fund generation consume it, rather than sitting stale for several days beforehand.

### Benchmarks

Two synthetic large-cap benchmarks (market cap ≥ $10B) are built from the FMP company screener API and stored in dedicated `benchmark` and `benchmark_holding` tables — separate from provider ETF data.

| Benchmark | Region | Coverage |
|-----------|--------|----------|
| US Large Cap Blend | US | All large-cap US stocks |
| Intl Large Cap Blend | International | All large-cap non-US stocks |

`benchmark_generator.run()` runs every Wednesday (right before `best_ideas_generator`/`funds_update`): paginates the FMP screener, upserts any new tickers into the `ticker` table, then stores market-cap-weighted holdings for both benchmarks.

Each `provider_etf` row has an optional `benchmark_id` FK pointing to the appropriate benchmark. When set, `best_ideas_generator` computes best ideas twice per ETF — once using the ETF's own holdings as the benchmark (`benchmark_mode = 'self'`) and once using the external benchmark universe (`benchmark_mode = 'full_universe'`). Both sets are stored in `best_idea`.

`fund.strategy.benchmark` (`'full_universe'` | `'self'`, default `'full_universe'`) controls which mode `funds_update` selects when building each fund's model portfolio.

#### Multi-ticker (share-class) company consolidation

A company can trade as more than one independently-listed ticker (e.g. Alphabet as `GOOGL`/`GOOG`). `ticker.master_ticker_id` groups these: `NULL` on the master row, pointing at the master's `id` on every sibling. `ticker.accumulated_market_cap` is populated only on the master, as the sum of the latest known market cap across it and all its siblings.

Detection (`modules/ticker/master.py::sync_master_tickers()`, run every Wednesday before `benchmark_generator`): primary match is `ticker.cik` (SEC Central Index Key, shared across a US company's share classes); tickers with no CIK (typically international listings) fall back to normalized-company-name matching. Either way, the master is elected once — highest market cap at election time, tie-broken by US-exchange preference then lowest id — and **frozen permanently**; a group that already has an established master never re-elects, regardless of how market caps move afterward.

Both `benchmark_generator` (building `benchmark_holding`) and `best_ideas_generator` (computing `etf_weight`/`benchmark_weight`) redirect a sibling ticker to its master and use `accumulated_market_cap` in place of the sibling's own market cap, so a company with multiple share classes is weighted once, at the company level, instead of being split across each listing. `modules/calc/model_fund.py::resolve_canonical_ticker_ids` is a thin `COALESCE(master_ticker_id, ticker_id)` lookup over this same persisted assignment. Live pipeline only — BT has no `full_universe`/`benchmark_holding` concept and isn't affected.

### Key Modules

| Path | Purpose |
|------|---------|
| `modules/core/db.py` | `DatabasePoolSingleton` — connection pools for live and BT DBs |
| `modules/core/api_stocks.py` | FinancialModelingPrep API client with token-bucket rate limiting (200 req/min) |
| `modules/core/sender.py` | Admin email notifications via Mailgun |
| `modules/calc/best_ideas.py` | Core algorithm: active weight = ETF% − benchmark market-cap% |
| `modules/calc/classification.py` | Scikit-learn GradientBoosting style classifier — **BT only** |
| `modules/calc/model_fund.py` | Fund strategy parsing and composition logic; `Strategy.benchmark` selects best-idea mode |
| `modules/parse/url.py` | Playwright scraper with anti-detection; replays recorded event sequences |
| `modules/parse/convert.py` | Excel/CSV parsing using provider `Mapping` config from DB |
| `modules/object/benchmark.py` | Dataclasses and CRUD for `benchmark` and `benchmark_holding` tables |
| `modules/cron/benchmark_generator.py` | FMP screener fetch, ticker upsert, blend benchmark holding storage |
| `modules/ticker/master.py` | Ticker profile refresh (cik/isin/name/etc.), master-ticker election/freeze, accumulated market cap refresh |
| `modules/object/_db_schema.sql` | Authoritative PostgreSQL schema |

### DB Access Pattern

All data models are `@dataclass` classes with their own CRUD functions. Rows are fetched using `psycopg.rows.class_row(ClassName)`. Example:

```python
with db_pool_instance.get_connection() as conn:
    with conn.cursor(row_factory=class_row(Ticker)) as cur:
        cur.execute('SELECT * FROM ticker WHERE symbol = %s', (symbol,))
        item = cur.fetchone()
```

Live uses `db_pool_instance`; BT uses `db_pool_instance_bt`. Never cross-import between live and BT object modules.

### Live vs BT Object Modules

BT has its own parallel set of dataclasses under `modules/bt/object/` that mirror the live `modules/object/` ones but connect to `best_ideas_bt`. When adding fields or logic to a live object, check if the BT equivalent also needs updating.

### Style Classification

- **Live**: ticker style (`style_type`, `cap_type`) is set directly from ETF holdings data via SQL — no ML model.
  - Primary: `ticker.update_style_from_categorization_etfs()` — uses `categorize_etf_holding` join
  - Fallback: `ticker.update_style_from_provider_etfs()` — uses `provider_etf.style_type` (value/growth only)
- **BT**: uses a GradientBoosting classifier trained from `categorize_ticker` table; `ticker.mark_style(classifier)` handles residual tickers.

### Web Scraping

Playwright with `playwright-stealth`. Provider/ETF scraping config (URL, CSS selectors, event sequences) is stored in the database `provider` and `provider_etf` tables. Events are recorded with the [Playwright CRX Chrome plugin](https://chromewebstore.google.com/detail/jambeljnbnfbkcpnoiaedcabbgmnnlcd) and stored as JSON arrays.

Pass `--headed` on the command line to run with a visible browser window when debugging scraping issues (see Dev/Prod DB switching above).

### DB Schema Migrations

Column additions follow the `x_d_shift_columns_template` pattern (5-step shift): add new column, add `_` copies of subsequent columns, UPDATE copies from originals, DROP originals, RENAME copies. See `modules/object/_migration_*.sql` for examples.

## Environment Variables

```
ENV_TYPE=development|production   # fallback when no --prod/--dev flag is passed
SECRET_DATABASE_USER=             # development (default)
SECRET_DATABASE_PASSWORD=
SECRET_DATABASE_HOST=
SECRET_DATABASE_PORT=
SECRET_DATABASE_NAME=best_ideas
SECRET_DATABASE_NAME_BT=best_ideas_bt   # backtesting DB — always dev, unaffected by --prod

SECRET_DATABASE_PROD_USER=        # production (used only with --prod)
SECRET_DATABASE_PROD_PASSWORD=
SECRET_DATABASE_PROD_HOST=
SECRET_DATABASE_PROD_PORT=
SECRET_DATABASE_PROD_NAME=

SECRET_MAILGUN_ENDPOINT=
SECRET_MAILGUN_API_KEY=
SECRET_MARKET_DATA_API_KEY=   # FinancialModelingPrep
```
