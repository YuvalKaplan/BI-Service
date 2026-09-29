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
etf_downloader        → scrapes provider websites (Playwright), resolves lines  → provider_etf_holding, ticker
                        (TickerResolver + FMP)
universe_screener     → FMP screener API; registers home-market lines daily;    → ticker (+ screener_listing on Wed)
                        Wednesday: stores the screen
ticker maintenance    → modules/ticker utilities over every ticker:              → ticker, ticker_value
  Tue–Sat               refresh (profiles) → valuation (values) → master (companies, cap, region) → style
  Sunday                categorize_downloader (style reference ETFs + FMP factors → categorize_ticker) → esg
universe_builder      → foreign lines, one row per company, duplicate guard      → universe_company
benchmark_generator   → forms each enabled `benchmark` row from the universe     → benchmark_holding
best_ideas_generator  → active weight algorithm                                  → best_idea (self + full_universe modes)
funds_update          → fund strategy composition                                → fund_holding, fund_holding_change, fund_analysis
```

`ticker_value` (price + USD market cap) is written by the valuation pass (`modules/ticker/valuation.py::run()`: every valid ticker in each active ETF's latest holdings from the last 7 days, plus the registered lines of a screen stored for the date — once per ticker, 5 in parallel, skipping tickers already valued for the date), via `modules/ticker/pricing.py::store_validated_ticker_value`, which validates against FMP historical endpoints and converts to USD with per-date FX rates. Resolution no longer stores values (a ticker held by several providers was valued once per provider). Values are stored under `pricing.latest_value_date()` — the latest completed trading day (before 17:00 ET the previous day, weekends stepped back to Friday) — so the Wednesday cron (~01:00–04:00 UTC) values Tuesday's close. Never `date.today()`: at that hour FMP has no data for it yet, and every US value would be withheld.

**Cron schedule** (weekday 0=Monday, UTC):
- Tue–Sat, listings in first, then ticker maintenance over every ticker: `etf_downloader.run()` (holdings, resolution) → `universe_screener.run(store=(weekday == 2))` (registers the ~2,500 home-market listings, ~1 min; on Wednesday also stores the screen) → `refresh.refresh_ticker_profiles()` (first, so values use current currencies and skip newly invalid tickers) → `valuation.run()` (~3,600 tickers, ~4,500 on Wednesday with the screen) → `master.sync_masters_and_company_data()` (master ticker sync + company market cap/region from the day's values, DB-only) → `style.assign_styles()` (after the sync, so siblings aren't classified on their own).
- Sun (ticker maintenance, weekly part): `categorize_downloader.run()` → `esg.refresh_all()`
- Wed (after the Tue–Sat steps), the generators: `universe_builder.run()` → `benchmark_generator.run()` → best ideas generation → fund updates. A failed step stops the rest: the FMP screener raises once its 3 attempts fail, and an empty universe or benchmark raises too. Each step reads its input from the database (the latest screen / universe on or before today), so any one can be re-run alone (`scripts/current_universe_screener.py` [`--register-only`], `data_fill_ticker_profile.py`, `current_ticker_values.py`, `data_fill_master_tickers.py`, `current_universe_builder.py`, `current_benchmark_generator.py`).

**Ticker utilities** (`modules/ticker/`, README "Ticker utilities"): registration (`resolver.py`; screener lines via `universe_screener.register_listings`) → profile refresh (`refresh.py`) → prices and market caps (`valuation.py` + `pricing.py`) → share-class consolidation (`master.py`, `company.py`, `identity.py`) → style (`style.py`) → ESG (`esg.py`, weekly). Stages call them; they don't live inside a stage. `resolver.py` and `esg.py` import each other as modules (new tickers get ESG at registration) — keep those imports module-level (`from modules.ticker import esg`), not name-level.

Benchmark generation runs on Wednesday so it's built the same day best ideas/fund generation consume it.

### Benchmarks

Synthetic benchmarks are defined by the rows of the `benchmark` table (`region`, `cap_type`, `style_type`, `market_cap_min`, `disabled`) and their weekly snapshots stored in `benchmark_holding` — separate from provider ETF data. Today there are two, both `large` / `blend` with `market_cap_min` $10B **in USD**:

| Benchmark | Region | Coverage |
|-----------|--------|----------|
| US Large Cap Blend | US | Universe companies with `region = 'US'` |
| Intl Large Cap Blend | International | Universe companies with `region = 'International'` |

They're built in three steps, each reading the previous one's output from the database:

1. **`modules/cron/universe_screener.py::run(screen_date, store)`** (Tue–Sat, after the holdings download) queries the FMP screener per exchange (`SCREENER_EXCHANGES`, LSE included). **FMP's `marketCapMoreThan` filters on the listing's local-currency cap** (verified: ¥10B admits $65M companies), so the threshold is converted per exchange (`_usd_rate`). The home-market lines are registered (`register_listings`: ticker upsert, the profile of a brand-new one at once for its currency/ISIN/CIK), so the daily ticker maintenance covers them. With `store` (Wednesday, the sim, by hand) every line is also stored in `screener_listing` (quote market cap + price in local currency, `line_type`, `ticker_id`) for `screen_date` (default `pricing.latest_value_date()`); the valuation pass then values its registered lines with the quote's share count as `reference_shares`. Foreign lines are only stored.
2. Ticker maintenance values them (`valuation.run()`) and the master sync (below, daily) links them to their companies.
3. **`modules/cron/universe_builder.py::run(screen_date, require_value)`** (Wednesdays) builds `universe_company` (one row per company: master at build time, region, USD company cap) from the latest stored screen: home lines at their latest value within `VALUE_WINDOW_DAYS` (5) up to the screen date — a market closed that day (JPX on a Japanese holiday) keeps its last close; the sim passes `require_value=False` and falls back to the latest value — then foreign lines, one row per company (`_one_row_per_company`: master's `company_market_cap`, else the listing's value), then `drop_duplicate_companies()`. Invalid tickers are left out. No cap floor here.

**The screener returns listings, not companies, and FMP stamps the whole company's cap on foreign lines, depositary receipts, even preferreds and notes** — any line not attached to its company counts the company twice. So a company enters only through its home-market ordinary listing (`universe_screener._classify`): non-equity lines (`company.is_non_equity_line`: preferred/notes/warrants/units/participation certificates, `-P?` symbols, Korean codes not ending in 0) and LSE International Order Book mirrors (`util.is_iob_line`: `0XXX` codes) are never used; home-market (`util.market_tier` ≤ 1), US-exchange, and unscreened-domicile lines (BM, KY, HU, … — `util.has_screened_home`) are `home`; other lines are `foreign` and admitted by the universe builder (after the master sync) only if they duplicate no admitted company (listing of one, shared ISIN, or `names_match` on `identity.core_name` within the same domicile) and are quoted in their exchange's currency (LSE mirrors carry the issuer's currency: Toyota's LSE line reports JPY) — they're registered then. `drop_duplicate_companies()` is the last safety net on the consolidated rows (`identity.duplicate_evidence`, home-anchored companies first, across both regions). Stats go to the cron email (`universe_screener.summary`, `universe_builder.summary`) and the sim prep report.

**`modules/cron/benchmark_generator.py::run(screen_date)`** only forms benchmarks: for every enabled `benchmark` row, the latest universe's companies (read through their current master, `current_companies`) of its `region`, with company cap ≥ `market_cap_min`, of its `style_type` (`blend`/`core`: any; `value`/`growth`: the company's `ticker.style_type`) — `select_holdings` — market-cap weighted and stored at the universe's screen date — Tuesday for the Wednesday run (`store_holdings`, replacing that date). Any empty benchmark raises before anything is stored. The sim backfill uses the same `select_holdings`/`store_holdings` per Wednesday, so `market_cap_min` applies to each date's caps.

Each `provider_etf` row has an optional `benchmark_id` FK pointing to the appropriate benchmark. When set, `best_ideas_generator` computes best ideas twice per ETF — once using the ETF's own holdings as the benchmark (`benchmark_mode = 'self'`) and once using the external benchmark universe (`benchmark_mode = 'full_universe'`). Both sets are stored in `best_idea`.

`fund.strategy.benchmark` (`'full_universe'` | `'self'`, default `'full_universe'`) controls which mode `funds_update` selects when building each fund's model portfolio.

#### Multi-ticker (share-class) company consolidation

A company can trade as more than one independently-listed ticker (e.g. Alphabet as `GOOGL`/`GOOG`, and `ABEA` on XETRA). `ticker.master_ticker_id` groups these: `NULL` on the master row, pointing at the master's `id` on every sibling (always one level — `repair_master_chains()` flattens chains/cycles).

Detection (`modules/ticker/master.py::sync_masters_and_company_data()`, its own step of the daily ticker maintenance, after the universe screener, the profile refresh and the valuation pass, before style assignment and the Wednesday universe builder and generators; DB-only, no FMP calls): `sync_master_tickers()` matches on `ticker.cik`; a no-CIK ticker whose normalized name matches exactly one CIK company joins that company's master (this also re-points a pre-existing no-CIK group); remaining no-CIK tickers group by exact normalized name (`util.name_key` — case/accents/punctuation ignored, legal suffixes deliberately *not* stripped, which could merge unrelated companies). Then `link_same_company()` merges groups on `modules/ticker/identity.py::link_evidence` (FMP names one company differently across listings, gives receipts their own ISIN and foreign lines no CIK): a shared ISIN with names agreeing on the first meaningful word (`first_word_agrees`; full `names_match` for generic words like "Banco") **or caps within 10%** (FMP sometimes attaches another company's ISIN — Seabridge Gold carrying Santander's — whose cap is far off); same-domicile `names_match` with caps within 2% on a common date; a depositary receipt whose `core_name` matches. Different CIKs never merge, except a dual listing (shared ISIN under the identical name: Rio Tinto plc/Ltd). Candidate pairs are blocked by ISIN and (domicile, first name token). Rejected ISIN matches go to the master-groups report. `name_tokens` folds accents. The unlink step keeps a sibling while it's tied (`_tied` — looser than link_evidence, so links don't churn on a week one listing lacks a fresh cap: same CIK, same ISIN with agreeing names or same domicile, identical name, same-domicile `names_match`/`core_name` match) to its master or any other group member; a CIK conflict with the master unlinks unless dual-listed. Finally `align_masters_to_primary()` makes every group's master its **primary listing** (the current master wins ties; a challenger needs a cap within `REALIGN_ACTIVE_DAYS` = 90) — masters aren't frozen, but market-cap moves never change them. Ids stored under an earlier master are resolved through the current one on read: `best_ideas_generator.get_benchmark_weights` (benchmark snapshots), `funds_update._to_current_masters` (previous fund holdings), `model_fund.resolve_canonical_ticker_ids` (best ideas).

**Company market cap and region** (`modules/ticker/company.py`, persisted by `master.refresh_company_data()`): FMP reports the **whole company's** cap on every listing, so listings are never summed — all share classes, listed or not, and Up-C LLC units (Carvana ~1.1B shares vs ~0.72B listed Class A; kept by decision, external listed-class caps differ). Listings are ordered (`company.ordered_listings`): active listings first (a market cap within the last 30 days — drops tickers left behind by a ticker change; generous so a listing priced only by the weekly screener doesn't flip), ordinary shares before preferred / note / depositary / when-issued / unit lines (`is_secondary_line` = `is_non_equity_line` or `is_depositary_line`) and before thin lines (`thin_lines`: `ticker.average_turnover` — FMP profile averageVolume × price, stored by the profile refresh and registration, `data_fill_ticker_turnover.py` — under `THIN_TURNOVER_SHARE` 5% of the company's busiest ordinary line on the same country's exchanges and currency, `turnover_group_key` — NYSE/NASDAQ/AMEX together, NSE with BSE, OTC/IOB by venue; FMP names some units/notes/preferreds exactly like the company: Southern's `SOMN` units, ANZ's `AN3PJ` capital notes, Comcast's `CCZ` debentures on NYSE against `CMCSA` on NASDAQ), then by `util.market_tier`: 0 the domicile's exchanges (`util.EXCHANGE_COUNTRY`, HK counts for CN), 1 a wider home market (`WIDER_HOME_COUNTRIES`: NL/LU holding companies on Paris/Milan/Brussels/Amsterdam/Madrid/Lisbon), 2 a US exchange, 3 other countries' exchanges, 4 OTC/IOB/unknown; then lines quoted in their exchange's currency before others (`is_foreign_currency_line` — HK RMB counters); the master, then lowest id. The first is the *primary listing*. `ticker.company_market_cap` (master only) = the latest cap of the first listing in that order that has one; `ticker.region` (every listing) = `'US'` if the primary listing is on NYSE/NASDAQ/AMEX (or on OTC/IOB for a US-domiciled company), else `'International'`.

**Currency**: market caps are converted to USD from the listing's own FMP currency (`util.listing_currency(exchange, ticker.currency)`, minor units like GBp normalized), else the exchange's — Compass on LSE and Jardine Matheson on SES report in USD, HK RMB counters in CNY, LSE mirrors in the issuer's currency. `ticker.upsert_by_symbol` COALESCEs currency/sector/industry/country/type_from (it used to NULL them when the screener re-upserted a cross-listed symbol). When the profile refresh changes a ticker's conversion currency, `pricing.resync_value_history` rewrites its stored history (else `compare_overlap` would withhold every new value and flag it invalid).

**FMP market-cap glitches**: `pricing.fetch_price_and_market_cap_history` (used by validation, `resync_value_history`, the refresh/gap scripts and the sim backfill) runs `clean_market_caps` on the native-currency caps before FX. `market_cap_outliers`: a ≥3× day-over-day cap move the price didn't make (implied share count jumps too; ≥20× regardless) cuts the series into runs (`_structural_outliers`: middle spikes/streaks first, then short edge runs); when that finds anything, or a ≥2× price-unmatched move / a new series (`_needs_reference`), the listing's quote share count arbitrates (`reference_shares` = profile marketCap / price, one API call; for a stored screen's lines (valuation pass) and in the sim the screener row's marketCap / price, free, passed as `reference_shares` through `store_validated_ticker_value`/`fetch_price_and_market_cap_history`) — values ≥`REFERENCE_FACTOR` (2.5×) off it are the glitches, whichever side is the majority (FMP's late-July share-count change broke Glovis's recent values but fixed Centrica's older ones; the Japanese banks' OTC lines are off by the yen rate since April) — unless everything it agrees with is a middle spike/streak (`_structural_outliers(middle_only=True)`), i.e. the profile is wrong itself (SABESP), or it agrees with nothing in an established (≥65 priced points) history; against a new line it wins (Uniper `UN01`). The profile refresh (`refresh.stored_cap_off_profile`) rewrites any history with a stored cap 2.5× off the profile's share count, passing it as the reference; `data_fill_ticker_value_refresh.py --profile-check` does it for all tickers at once. Offsets below that (1.15–2.5× on each of the last 10 stored values, `refresh.stored_shares_off_profile`) go to FMP's quarterly financials (`api_stocks.get_quarterly_weighted_shares`): if they agree with the profile (5%), the profile's count becomes `ticker.verified_shares` (migration 13; `refresh.verify_share_count`, weekly, and `--shares-check`) and the history is rewritten — RKT's history carries 3.79B shares against 2.82B. `clean_market_caps(verified_shares=...)` sets every value `SHARE_COUNT_TOLERANCE` (1.15×) off it to price × the verified count on **every** fetch (valuation `ValueTarget.verified_shares`, resync, the gap/refresh scripts, the sim fallback), or the daily validation would flag the ticker invalid against the rewritten history. The count follows the profile while the financials agree and is cleared when they don't (then the history is rewritten from FMP again). Glitches are repaired as price × the nearest good values' share count, dropped when the price is also ≥20× off. Validation fetches `OUTLIER_CONTEXT_DAYS` (365) of context but compares only the last 21 days. `data_fill_ticker_value_refresh.py --outliers` rewrites stored history that still holds glitches. Benchmarks split US/International by `region`, and the fund region filter (`model_fund._eligibility_masks`) uses the same `region`.

Both the universe builder (feeding `benchmark_holding`) and `best_ideas_generator` (computing `etf_weight`/`benchmark_weight`) redirect a sibling ticker to its master and use `company_market_cap` in place of the listing's own market cap, so a company is weighted once, at the company level. `best_ideas_generator` measures a company **as of the holdings date**: `company.company_caps_as_of` takes the first listing in primary order with a value within the window around that date (`best_ideas_generator.prepare_etf_inputs` ±5 days; `funds_update.build_shared_context` ±10 days, feeding the fund's large-cap filter and market-cap weighting), falling back to the stored `company_market_cap`. So sim dates use that date's values, as the sim benchmark does. `modules/calc/model_fund.py::resolve_canonical_ticker_ids` is a thin `COALESCE(master_ticker_id, ticker_id)` lookup over this same persisted assignment. Live pipeline only — BT has no `full_universe`/`benchmark_holding` concept and isn't affected.

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
| `modules/object/screener_listing.py`, `modules/object/universe_company.py` | The stored weekly screen (one row per screener line) and large-cap company universe (one row per company) |
| `modules/cron/universe_screener.py` | FMP screener fetch per exchange, line classification, home-market line registration (`register_listings`, daily); Wednesday: `screener_listing` storage and value validation |
| `modules/cron/universe_builder.py` | Large-cap company universe from the stored screen: foreign-line admission, one row per company, duplicate guard → `universe_company` |
| `modules/cron/benchmark_generator.py` | Forms every enabled `benchmark` row (region / `market_cap_min` / style) from the stored universe → `benchmark_holding` |
| `modules/cron/categorize_downloader.py` | Scrapes style reference ETFs into `categorize_ticker` (with FMP factors) |
| `modules/ticker/style.py` | `assign_styles()`: the style chain (CAT_ETF → PROVIDER_ETF → MODEL) for unclassified companies, run after the daily master sync |
| `modules/ticker/esg.py` | `populate_esg` (one ticker; new tickers at registration) and `refresh_all()` (weekly, every valid company — masters/standalone only) |
| `modules/sim/orchestrator.py`, `modules/sim/benchmark_generator.py` | Fund simulation loop and historical benchmark backfill |
| `modules/ticker/master.py` | Master-ticker grouping/same-company linking/realignment to the primary listing/repair, company market cap + region refresh |
| `modules/ticker/refresh.py` | Ticker profile refresh (cik/isin/name/currency/validity), value-history resync on a currency change or a history off the profile's share count (`stored_cap_off_profile`) |
| `modules/ticker/company.py` | Primary-listing rule behind the master, `company_market_cap` and `region`; non-equity / depositary line detection |
| `modules/ticker/identity.py` | Evidence that two listing groups are one company (shared ISIN, name + cap, depositary receipt, dual listing) — master linking and the benchmark duplicate guard |
| `modules/ticker/resolver.py` | Resolves provider holding lines to `ticker` rows (US: symbol; non-US: ISIN, else symbol search, else verified name search) |
| `modules/ticker/valuation.py` | The valuation pass: which tickers are in use (recent ETF holdings + a stored screen), each valued once for `pricing.latest_value_date()`, in parallel |
| `modules/ticker/pricing.py` | Validated `ticker_value` storage (FMP history cross-check, grace period, invalid flagging), FMP market-cap glitch repair, historical FX → USD, and `latest_value_date()` |
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

- **Live** (`modules/ticker/style.py::assign_styles()`, Tue–Sat right after the master sync so a new listing is grouped before it could be classified on its own; also `scripts/current_categorize_tickers.py`), only for tickers with `style_type IS NULL`, skipping share-class siblings (`master_ticker_id` set); `ticker.type_from` records the source:
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
