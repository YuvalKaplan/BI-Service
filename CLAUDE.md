# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository. `README.md` has the human-readable, step-by-step description of every pipeline stage, all scripts, environment variables, Docker and MCP setup — keep it in sync when behaviour described there changes.

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

# Fund methodology export (ETFs -> benchmark -> best ideas -> fund) for one fund/date
python scripts/report_fund_methodology.py

# Update requirements
pip freeze > requirements.txt
```

No automated test suite exists. Validation is done by running the cron or BT pipelines directly.

All script/pipeline output (reports, sim results, methodology exports, BT CSVs) goes under `.output/` (git-ignored); downloaded provider holdings files go under `.output/downloads/` (`scripts/download_and_save_all_providers.py`, `scripts/single_provider_etf.py`, `modules/parse/convert.py::FILE_FOLDER`). New scripts should follow the same convention.

### Dev/Prod DB switching

`service_cron.py` and every script in `scripts/` (except `db_sync_dev_from_prod.py`, which opens both databases itself) default to the **development** database (`db_pool_instance`, live pipeline only). Pass `--prod` to point that same pool at production instead, or `--dev` to force development even if `ENV_TYPE=production` is set in the environment:

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

- **Live** (`service_cron.py` → `modules/cron/`): scheduled daily/weekly data ingestion and analysis
- **Backtesting** (`modules/bt/run.py` → `modules/bt/`): multi-year historical study on purchased holdings data, with simulated accounts and performance

Each pipeline has its own PostgreSQL database (`best_ideas` and `best_ideas_bt`) and its own set of object modules with independent dataclasses and DB connections.

A third mode, the **simulation** (`scripts/sim_prep_data.py` → `scripts/sim_benchmark.py` → `scripts/sim_fund.py`, code in `modules/sim/`), is *not* a separate pipeline: it replays the live `best_ideas_generator.run(as_of_date=...)` and `funds_update.activate_fund` week by week against the live (dev) database, over already-collected holdings. `sim_fund.py` erases the target fund's `fund_holding`/`fund_holding_change`/`fund_analysis` first and refuses to run against production. Keep live code paths `as_of_date`-aware so the sim keeps working.

### Live Pipeline Flow

```
etf_downloader        → scrapes provider websites (Playwright), resolves lines  → provider_etf_holding, ticker, ticker_value
                        (TickerResolver + FMP), then assigns style to new tickers → ticker.style_type
ticker.master         → profile refresh; master-ticker sync + accumulated caps   → ticker
categorize_downloader → scrapes style reference ETFs + FMP factors               → categorize_etf_holding, categorize_ticker
esg_update            → FMP ESG disclosure/rating                                → ticker.esg_qualified, esg factors
benchmark_generator   → FMP screener API                                         → benchmark_holding
best_ideas_generator  → active weight algorithm                                  → best_idea (self + full_universe modes)
funds_update          → fund strategy composition                                → fund_holding, fund_holding_change, fund_analysis
```

`ticker_value` (price + USD market cap) is written whenever a ticker is resolved or screened, via `modules/ticker/pricing.py::store_validated_ticker_value`, which validates against FMP historical endpoints and converts to USD with per-date FX rates.

**Cron schedule** (weekday 0=Monday, UTC):
- Tue–Sat: ETF holdings download (incl. ticker resolution/values and style assignment for unclassified tickers), ticker profile refresh
- Sun: Categorization ETFs, ESG update
- Wed (after the Tue–Sat steps): master ticker sync + accumulated market cap refresh (`modules/ticker/master.py`), benchmark blend holdings (`benchmark_generator.run()`), best ideas generation, fund updates

Benchmark generation runs on Wednesday so it's built the same day best ideas/fund generation consume it.

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
| `modules/cron/best_ideas_generator.py` | Core algorithm: active weight = ETF% − benchmark weight (`prepare_etf_inputs` → `compute_active_weights` → `select_best_ideas`) |
| `modules/calc/classification.py` | Scikit-learn GradientBoosting value/growth classifier — used by live (`etf_downloader`) and BT |
| `modules/calc/esg.py` | ESG qualification rules (risk rating / ESG score / governance score thresholds) |
| `modules/calc/model_fund.py` | Fund strategy parsing and composition logic; `Strategy.benchmark` selects best-idea mode |
| `modules/parse/download.py` | Per-provider collection: scrape → parse → resolve → store holdings |
| `modules/parse/url.py` | Playwright scraper with anti-detection; replays recorded event sequences |
| `modules/parse/convert.py` | Excel/CSV parsing using provider `Mapping` config from DB |
| `modules/object/benchmark.py` | Dataclasses and CRUD for `benchmark` and `benchmark_holding` tables |
| `modules/cron/benchmark_generator.py` | FMP screener fetch, ticker upsert, blend benchmark holding storage |
| `modules/cron/categorize_downloader.py` | Scrapes style reference ETFs into `categorize_ticker` (with FMP factors) |
| `modules/cron/esg_update.py` | Weekly ESG refresh for every valid company (masters/standalone only) |
| `modules/sim/orchestrator.py`, `modules/sim/benchmark_generator.py` | Fund simulation loop and historical benchmark backfill |
| `modules/ticker/master.py` | Ticker profile refresh (cik/isin/name/etc.), master-ticker election/freeze, accumulated market cap refresh |
| `modules/ticker/resolver.py` | Resolves provider holding lines to `ticker` rows (US: symbol; non-US: ISIN, else symbol search, else verified name search) |
| `modules/ticker/pricing.py` | Validated `ticker_value` storage (FMP history cross-check, grace period, invalid flagging) and historical FX → USD |
| `modules/object/fund_analysis.py` | Per-fund snapshot of every constituent ETF company's active-weight calculation |
| `modules/report/fund_methodology.py` | Methodology export (`scripts/report_fund_methodology.py`) → `.output/methodology/` |
| `modules/object/_db_schema.sql` | Authoritative PostgreSQL schema |

### Holding lines, duplicates and ticker resolution

Provider files are resolved line by line to `ticker_id` (`TickerResolver`). Non-US lines without an ISIN fall back to FMP symbol search and then name search; a name candidate is only accepted when `ticker.util.names_match` holds (every token of the shorter name matches, covering more than half of the longer one), never on a single shared word — loose matching used to map unrelated holdings onto one ticker.

A ticker can still appear on several lines of one ETF/date. `provider_etf_holding.aggregate_holdings()` (used by `best_ideas_generator.prepare_etf_inputs`) sums the lines when they all imply the same price (`DUP_PRICE_TOLERANCE`), and **quarantines** all of that ticker's lines when they don't (they're most likely different securities mis-resolved to one ticker) — reported as `QUARANTINED DUPLICATES` per ETF. `scripts/debug_holding_duplicates.py` reports current duplicate groups.

### Fund analysis snapshot and methodology export

On every fund recalculation (live `funds_update` and sim), `funds_update._record_fund_analysis` writes `fund_analysis`: for each ETF the fund draws on, every company's market cap used, `master_used` marker, ETF weight, benchmark weight (with `benchmark_id`/`benchmark_date`; NULL for `self`), delta, rank and a note on why it did or didn't become a best idea. It's recomputed with the same `best_ideas_generator` helpers (`prepare_etf_inputs` → `compute_active_weights` → `select_best_ideas` → `build_analysis_rows`) and cross-checked against `best_idea`. `scripts/report_fund_methodology.py` (read-only; edit `fund_id`/`as_of_date` at the top, default = inception) turns it into `README.md`, `etfs/<etf_name>_<etf_id>.xlsx`, `benchmark.xlsx`, `best_ideas.xlsx` and `fund.xlsx` under `.output/methodology/<fund>_<id>_<date>/`.

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

- **Live** (end of `etf_downloader.run()`, and `scripts/current_categorize_tickers.py`), only for tickers with `style_type IS NULL`, skipping share-class siblings (`master_ticker_id` set); `ticker.type_from` records the source:
  1. `CAT_ETF` — `ticker.update_style_for_unclassified()`: match on `categorize_ticker` (symbol + exchange), sets style and cap
  2. `PROVIDER_ETF` — `ticker.update_style_from_provider_etfs()`: held by a provider ETF whose `style_type` is value/growth
  3. `MODEL` — `classification.mark_style(classifier, ticker)`: GradientBoosting trained on `categorize_ticker` factors; tickers with no FMP factor data get `style_factors_failed_at` and are retried after 30 days
  
  `ticker.update_style_from_categorization_etfs()` exists but is not called anywhere.
- **BT**: same classifier approach via its own modules; `ticker.mark_style(classifier)` handles residual tickers.

### Web Scraping

Playwright with `playwright-stealth`. Provider/ETF scraping config (URL, CSS selectors, event sequences) is stored in the database `provider` and `provider_etf` tables. Events are recorded with the [Playwright CRX Chrome plugin](https://chromewebstore.google.com/detail/jambeljnbnfbkcpnoiaedcabbgmnnlcd) and stored as JSON arrays.

Pass `--headed` on the command line to run with a visible browser window when debugging scraping issues (see Dev/Prod DB switching above).

### DB Schema Migrations

Column additions follow the `x_d_shift_columns_template` pattern (5-step shift; the template is a stored procedure in `_db_schema.sql`): add new column, add `_` copies of subsequent columns, UPDATE copies from originals, DROP originals, RENAME copies. One-off changes go in `modules/object/_migration_<n>.sql`, are applied to dev then prod by hand, and the file is deleted once applied to both; update `_db_schema.sql` to match.

## Environment Variables

```
ENV_TYPE=development|production   # fallback when no --prod/--dev flag is passed (Docker image sets production)
PYTHONPATH=<project root>         # so `modules.*` imports resolve when running scripts
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
SECRET_HOLDINGS_DATA_API_KEY= # FactSet — BT one-off historical holdings download only
```

Never write credentials into tracked files: `.env`, `.mcp.json` and `.claude/` are git-ignored and contain real passwords.
