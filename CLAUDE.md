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
python scripts/_categorization.py
python scripts/_single_provider.py
python scripts/_stock_info.py

# Update requirements
pip freeze > requirements.txt
```

No automated test suite exists. Validation is done by running the cron or BT pipelines directly.

## Architecture

**Two separate pipelines share a codebase:**

- **Live** (`service_cron.py` → `modules/cron/`): scheduled weekly data ingestion and analysis
- **Backtesting** (`modules/bt/run.py` → `modules/bt/`): historical simulation over a date range

Each pipeline has its own PostgreSQL database (`best_ideas` and `best_ideas_bt`) and its own set of object modules with independent dataclasses and DB connections.

### Live Pipeline Flow

```
etf_downloader     → scrapes provider websites (Playwright) → provider_etf_holding
stocks_downloader  → FinancialModelingPrep API              → ticker, ticker_value
stocks_categorize  → scrapes style/ESG ETFs                 → ticker (style_type, esg_qualified)
best_ideas_generator → active weight algorithm              → best_idea
funds_update        → fund strategy composition             → fund_holding
```

**Cron schedule** (weekday 0=Monday):
- Tue–Sat: ETF holdings download, stock data download, categorization
- Tue–Thu: Best ideas generation, fund updates

### Key Modules

| Path | Purpose |
|------|---------|
| `modules/core/db.py` | `DatabasePoolSingleton` — connection pools for live and BT DBs |
| `modules/core/api_stocks.py` | FinancialModelingPrep API client with token-bucket rate limiting (200 req/min) |
| `modules/core/sender.py` | Admin email notifications via Mailgun |
| `modules/calc/best_ideas.py` | Core algorithm: active weight = ETF% − benchmark market-cap% |
| `modules/calc/classification.py` | Scikit-learn GradientBoosting style classifier — **BT only** |
| `modules/calc/model_fund.py` | Fund strategy parsing and composition logic |
| `modules/parse/url.py` | Playwright scraper with anti-detection; replays recorded event sequences |
| `modules/parse/convert.py` | Excel/CSV parsing using provider `Mapping` config from DB |
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

Set `headless=False` in `url.py` when debugging scraping issues.

### DB Schema Migrations

Column additions follow the `x_d_shift_columns_template` pattern (5-step shift): add new column, add `_` copies of subsequent columns, UPDATE copies from originals, DROP originals, RENAME copies. See `modules/object/_migration_*.sql` for examples.

## Environment Variables

```
ENV_TYPE=development|production
SECRET_DATABASE_USER=
SECRET_DATABASE_PASSWORD=
SECRET_DATABASE_HOST=
SECRET_DATABASE_PORT=
SECRET_DATABASE_NAME=best_ideas
SECRET_DATABASE_NAME_BT=best_ideas_bt
SECRET_MAILGUN_ENDPOINT=
SECRET_MAILGUN_API_KEY=
SECRET_MARKET_DATA_API_KEY=   # FinancialModelingPrep
```
