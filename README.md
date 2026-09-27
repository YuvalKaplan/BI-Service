# Best Ideas - Services

Services that collect the published holdings of actively managed ETFs, work out which stocks each manager is most convinced about (their "best ideas"), and turn those ideas into model fund portfolios.

The same codebase runs in three ways:

| Mode | Entry point | Database | Purpose |
|------|-------------|----------|---------|
| **Live** | `service_cron.py` | `best_ideas` (dev or prod) | Scheduled daily job: collects data and, once a week, rebuilds benchmarks, best ideas and fund holdings. |
| **Simulation (sim)** | `scripts/sim_*.py` | `best_ideas` (dev) | Replays the live best-ideas and fund logic week by week over the holdings already collected, to see how a fund would have evolved. |
| **Backtesting (BT)** | `modules/bt/run.py` | `best_ideas_bt` | Long-range historical study (years) on purchased historical holdings data, with simulated accounts and performance vs. benchmarks. |

---

## Contents

- [How the live pipeline works](#how-the-live-pipeline-works)
  - [Weekly schedule](#weekly-schedule)
  - [1. Providers and ETFs](#1-providers-and-etfs)
  - [2. Holdings collection](#2-holdings-collection)
  - [3. Ticker resolution](#3-ticker-resolution)
  - [4. Prices and market caps](#4-prices-and-market-caps)
  - [5. Ticker profile maintenance](#5-ticker-profile-maintenance)
  - [6. Style categorization](#6-style-categorization-value--growth)
  - [7. ESG qualification](#7-esg-qualification)
  - [8. Share-class consolidation (master tickers)](#8-share-class-consolidation-master-tickers)
  - [9. Benchmark generation](#9-benchmark-generation)
  - [10. Best ideas](#10-best-ideas)
  - [11. Fund holdings formation](#11-fund-holdings-formation)
  - [12. Fund analysis snapshot](#12-fund-analysis-snapshot)
  - [13. Logging and notifications](#13-logging-and-notifications)
- [Simulation](#simulation)
- [Backtesting](#backtesting)
- [Scripts](#scripts)
- [Environment variables](#environment-variables)
- [Docker and hosting](#docker-and-hosting)
- [Python](#python)
- [Database](#database)
- [Playwright](#playwright)
- [Claude Code and the database MCP servers](#claude-code-and-the-database-mcp-servers)

---

## How the live pipeline works

```
Provider websites ──(Playwright)──► ETF holdings files ──► parse ──► resolve to tickers ──► provider_etf_holding
                                                                        │
                                   FinancialModelingPrep (FMP) API ◄────┘  profiles, prices, market caps, ESG, factors
                                                                        │
Categorization ETFs ──► categorize_ticker ──► ticker.style_type ◄───────┤
                                                                        ▼
FMP screener ──► benchmark_holding (US / Intl large-cap blend)     ticker, ticker_value
                          │                                             │
                          └──────────────► best_idea ◄──────────────────┘
                                              │
                                              ▼
                              fund_holding, fund_holding_change, fund_analysis
```

### Weekly schedule

`service_cron.py` runs once a day. What it does depends on the weekday (UTC):

| Day | Steps |
|-----|-------|
| Tue – Sat | Holdings collection from all providers (steps 2–4), style classification of new tickers (step 6), ticker profile refresh (step 5) |
| Sun | Categorization ETF download (step 6), ESG refresh (step 7) |
| Wed | *in addition to the Tue–Sat steps:* benchmark generation (step 9, which runs the master ticker sync of step 8 right after the screener) → best ideas (step 10) → fund updates (steps 11–12) |
| Mon | Nothing |

Collection runs Tue–Sat because providers generally publish the previous trading day's holdings, so those runs pick up Monday to Friday. The Wednesday steps run in that order on purpose: the benchmark is built on the same day the best ideas and funds consume it, and from freshly consolidated companies – the master sync runs inside benchmark generation, after the screener, so listings first seen that day are already linked to their companies. If a step fails the cron stops and emails the admin: in particular the FMP screener is retried 3 times and then fails the run, and an empty benchmark (no market cap could be validated) fails it too, so best ideas and funds never run on a partial or missing benchmark.

When the run finishes (or fails), an email summary goes to the admins (step 13).

### 1. Providers and ETFs

Everything that drives collection is configuration in the database, not code:

- **`provider`** – a fund manager's website (e.g. JP Morgan, Capital Group): start URL, default file format and column mapping, and any browser events needed to reach the download (cookie banners, "I am an investor" prompts…).
- **`provider_etf`** – one active ETF of that provider: its own URL/events/mapping when they differ from the provider's, `region` (`US` / `International`), `style_type` and `cap_type` as described by the manager, and an optional `benchmark_id` pointing at one of the synthetic benchmarks (step 9).
- **`fund`** – a model fund we build, with its `strategy` stored as JSON (step 11).

Disabling a provider or ETF in the database removes it from every stage.

### 2. Holdings collection

`modules/cron/etf_downloader.py` → `modules/parse/download.py`

1. All active providers are processed in parallel (5 at a time).
2. For each provider, a headless Chromium browser (Playwright with stealth settings to look like a normal Chrome) opens the provider page, replays the recorded events, and triggers each ETF's holdings download. See [Playwright](#playwright) for how pages and events are configured. A failed scrape is retried up to 3 times.
3. The downloaded file (Excel `.xls`/`.xlsx` or CSV) is parsed with the ETF's **mapping** (`modules/parse/convert.py`): which sheet, which header row, which columns hold ticker / ISIN / name / shares / market value / weight, and where the holdings date is (in the file, the file name, or on the web page).
4. Each line is resolved to a row in the `ticker` table (step 3). Lines that can't be resolved are counted as "problem tickers" and dropped.
5. The resolved lines are stored in `provider_etf_holding` and the ETF's `last_downloaded` is stamped.

The admin email lists, per provider, how many of its ETFs were downloaded.

### 3. Ticker resolution

`modules/ticker/resolver.py`

Each holding line is matched to a company using the FinancialModelingPrep (FMP) API:

- **US ETFs** – by symbol, via the FMP company profile.
- **Non-US ETFs** – by **ISIN** when the file has one. Without an ISIN, by FMP symbol search, accepting a candidate only when its company name matches the holding's name; failing that, by FMP name search, again only with a verified name match.

  A name match requires every meaningful word of the shorter name to match the longer one and cover more than half of it (legal suffixes such as *Inc*, *PLC*, *Holdings* are ignored). A single shared word is never enough – loose matching would otherwise map unrelated holdings onto the same ticker.

When a profile is found, the ticker is inserted or updated with its identifiers (ISIN, CUSIP, CIK), name, exchange, sector, industry, country and currency. It's **marked invalid** (and ignored from then on) when it is crypto, has no company name or market cap, or its name identifies it as a fund, ETF, trust or index. New tickers get their ESG data straight away (step 7).

Results are cached for the duration of a run so a stock held by many ETFs costs one lookup.

**Duplicate lines.** A ticker can still appear on several lines of one ETF on one date. When best ideas are computed, those lines are summed if they all imply the same share price (within 2%) – they are lots of the same security. If the prices disagree, they are most likely different securities that resolved to the same ticker, so all of that ticker's lines are **quarantined** (excluded) and reported.

### 4. Prices and market caps

`modules/ticker/pricing.py`

Every time a ticker is resolved (and for every company in the benchmark screener), its price and market cap for the latest completed trading day are stored in `ticker_value` (before 17:00 New York time that is the previous day).

FMP's profile endpoint occasionally returns wrong values, so the numbers are taken from FMP's **historical** price and market-cap endpoints and **validated**: the last 21 days fetched now are compared against what we stored before. If any overlapping day differs by more than 0.5% (price) or 1% (market cap), today's value is withheld. If a ticker has gone 5 days without a good value, it's marked invalid with the mismatch details.

All market caps are converted to **USD** using the historical exchange rate for that same date, based on the currency of the exchange the stock trades on.

### 5. Ticker profile maintenance

`modules/ticker/master.py::refresh_ticker_profiles`

Tue–Sat, every ticker whose profile hasn't been checked for a week is refreshed from FMP (ISIN, CUSIP, CIK, name, sector, industry, country, currency, actively trading). A ticker is marked invalid if its profile can't be fetched, turns out to be crypto or a fund/ETF, or is no longer actively trading. A ticker that checks out again has its invalid flag cleared.

### 6. Style categorization (value / growth)

Funds can be restricted to value or growth stocks, so each company needs a `style_type`.

**Reference data (Sundays)** – `modules/cron/categorize_downloader.py`

A list of index-style ETFs with a known style and cap size (e.g. a large-cap value index ETF) is kept in `categorize_etf`. Their holdings are scraped the same way as provider ETFs, and every constituent is stored in `categorize_ticker` with that ETF's style and cap type, plus a set of fundamental factors from FMP (growth rates, margins, valuation ratios, yields).

**Assigning style to tickers (Tue–Sat, after collection)** – in order of preference, only for tickers with no style yet (the `type_from` column records the source):

1. `CAT_ETF` – the ticker is a constituent of a categorization ETF (same symbol and exchange): take its style and cap type.
2. `PROVIDER_ETF` – the ticker is held by a provider ETF that describes itself as value or growth: take that style.
3. `MODEL` – a gradient-boosting classifier (`modules/calc/classification.py`) is trained on the categorized constituents' factors (market cap, sector, industry, growth rates, margins, P/E, P/B, yields…) and predicts value or growth for the rest. Tickers without factor data are retried after 30 days.

Style is a company-level attribute: only master and standalone tickers are classified (share-class siblings use their master's style – see step 8).

### 7. ESG qualification

`modules/cron/esg_update.py`, `modules/calc/esg.py`

On Sundays (and immediately for any new ticker), the FMP ESG disclosure and ESG risk rating of every valid company are fetched. A company is `esg_qualified` when every factor that is available passes:

| Factor | Passing |
|--------|---------|
| ESG risk rating | `AAA`, `AA`, `A`, `BBB`, `BB` or `B` |
| ESG score | ≥ 50 |
| Governance score | ≥ 50 |

A company with no ESG data at all is not qualified. The raw factors are stored alongside the flag. Funds with `esg_only` only pick qualified companies.

### 8. Share-class consolidation (master tickers)

`modules/ticker/master.py::sync_masters_and_company_data` – Wednesdays, inside benchmark generation (right after the screener)

Some companies trade as more than one listing (Alphabet as `GOOGL` and `GOOG`, and on XETRA as `ABEA`; Samsung on KSC, LSE and Vienna). Left alone they would be weighted twice and could both be picked by a fund. They are grouped instead:

- Tickers sharing an SEC **CIK** form a group. A ticker with no CIK (typically a foreign listing) whose normalized name matches exactly one CIK company joins that company – this is how a US company's German or London listings end up under its US master. Other no-CIK tickers are grouped by identical normalized name.
- The first time a group is seen, a **master** is elected: the company's primary listing (below), else the highest market cap, then lowest id. The master is then **frozen** – it never changes as market caps move. Every other member points at it through `ticker.master_ticker_id`. (The one exception: an existing no-CIK group is moved under the CIK company it turns out to belong to.)
- A sibling whose CIK or name no longer matches its master (e.g. after a profile correction) is unlinked first, so it can be regrouped in the same run. Chained or circular links are flattened.
- **Primary listing** – a listing on an exchange in the company's home country (HK counts as home for Chinese companies); otherwise a US listing (for US-listed, foreign-domiciled companies such as Eaton or Medtronic); otherwise the master.
- `ticker.company_market_cap` (master only) is the primary listing's latest market cap. FMP reports the **whole company's** market cap on every listing, so listings are never summed.
- `ticker.region` (every listing) is the company's region: `US` when the primary listing trades on NYSE/NASDAQ/AMEX, else `International`. TSM, ASML, SAP and Shopify are International; Eaton, Medtronic, Linde and Alphabet's XETRA listing are US.

Benchmarks, best ideas and funds all work at company level: a sibling is replaced by its master, the company market cap is used instead of a single listing's, and US/International is decided by `ticker.region`.

### 9. Benchmark generation

`modules/cron/benchmark_generator.py` – Wednesdays

Two synthetic benchmarks represent the investable large-cap universe (company market cap ≥ **$10B in USD**):

| Benchmark | Universe |
|-----------|----------|
| US Large Cap Blend | large-cap companies with `ticker.region = 'US'` |
| Intl Large Cap Blend | large-cap companies with `ticker.region = 'International'` |

1. The FMP company screener is paged through once per exchange: NYSE, NASDAQ, AMEX, TSX and 35 international exchanges (Japan, Germany, Hong Kong, Australia, Switzerland, France, China, India, Taiwan, Korea, the Nordics, …). FMP compares `marketCapMoreThan` against each listing's **local-currency** market cap, so the $10B threshold is first converted into the exchange's currency at the latest FX rate (an exchange with no known currency is skipped). The log shows each exchange's local threshold and smallest company returned, in USD.
2. Each company is matched to (or added to) the `ticker` table and its market cap is validated as in step 4. Companies whose value can't be validated this week are left out.
3. The master-ticker sync (step 8) runs here, so listings first seen today join their company and company caps reflect today's values.
4. Each company is represented once, by its master, at its company market cap; companies below $10B in USD are dropped.
5. Companies are split US / International by `ticker.region` and each is weighted by market cap (`weight = market cap / total`). The snapshot is stored in `benchmark_holding` for today's date.

### 10. Best ideas

`modules/cron/best_ideas_generator.py` – Wednesdays

A manager's best ideas are the stocks they hold at a **higher weight than the market would**. For each active ETF:

1. **Holdings** – the latest holdings downloaded in the last 7 days, with duplicate lines summed or quarantined (step 3).
2. **Market caps** – for each holding, the latest market cap within 5 days of the holdings date. Holdings without one are reported as stale. If fewer than **95%** of the holdings have a market cap, the ETF is skipped for the week and reported.
3. **Company level** – siblings are folded into their master and the ETF's exposure across share classes is summed.
4. **Active weight** for each company:

   ```
   etf_weight       = company's market value in the ETF / ETF total market value
   benchmark_weight = company's weight in the reference universe
   delta            = etf_weight − benchmark_weight
   ```

   Two reference universes are used, and both results are stored in `best_idea` with a `benchmark_mode`:

   | Mode | benchmark_weight | When |
   |------|------------------|------|
   | `self` | the company's market cap ÷ total market cap of the ETF's own holdings (what the ETF would look like if it were market-cap weighted) | always |
   | `full_universe` | the company's weight in the ETF's linked benchmark (step 9); 0 if not in it | only when the ETF has a `benchmark_id` |

5. **Selection** – companies with a positive delta, ranked from highest delta down. A delta above **20%** is treated as abnormal and dropped. The top **10** per ETF are stored, with their rank.

Problems (no recent download, stale holdings, quarantined duplicates, insufficient coverage, errors) are recorded in `batch_run_log` and listed in the admin email.

### 11. Fund holdings formation

`modules/cron/funds_update.py`, `modules/calc/model_fund.py` – Wednesdays, after best ideas

Each fund is defined by a JSON **strategy**, for example:

```json
{
  "holdings": 30,
  "allocation": "market_cap",
  "benchmark": "full_universe",
  "cap":    { "name": "large" },
  "style":  { "name": "blend", "value": 50, "growth": 50 },
  "region": { "name": "Global", "split": { "US": 70, "Non-US": 30 } },
  "provider_etfs": [12, 15, 87],
  "esg_only": false,
  "ranking_from": 1,
  "ranking_to": 3,
  "recalc_frequency_days": 7
}
```

| Field | Meaning |
|-------|---------|
| `holdings` | number of stocks in the fund |
| `allocation` | `market_cap` or equal weighting (any other value) |
| `benchmark` | which best ideas to use: `full_universe` (default) or `self` |
| `cap.name` | `large` (≥ $10B), `mid_small` (< $10B), or anything else for no filter |
| `style` | `value`, `growth`, or `blend`/`core` for no filter. A `blend` with `value`/`growth` percentages fills those shares of the fund from each style separately. |
| `region` | `US`, `International`, or a `split` with `US`/`Non-US` percentages; omitted = no filter |
| `provider_etfs` | limit to these ETFs' ideas (empty = all ETFs) |
| `exchanges` | limit to stocks on these exchanges |
| `esg_only` | only ESG-qualified companies |
| `ranking_from` / `ranking_to` | which ranks of each ETF's best ideas count (e.g. 1–3 = each manager's top three) |
| `recalc_frequency_days` | the fund is skipped until this many days have passed since its last recalculation |

**Building the candidate list.** All stored best ideas (latest per ETF) are filtered by the strategy:

- **Region** – `US` takes ideas from US ETFs in companies whose `ticker.region` is `US`; `International` takes ideas from international ETFs in companies whose region is `International` (region = primary listing, see step 8). With a split, the international list is built first and those companies are excluded from the US list.
- **Cap** – for a `large` fund, a stock that has fallen below $10B is still allowed *only if the fund already holds it*, so it isn't sold for that reason alone; it can never be a new buy.
- **Style, exchanges, ESG** as configured.

The candidates are then combined per company: its best (lowest) rank across the ETFs on its latest date, the number of ETFs that picked it (**appearances**), its highest delta, and the ETF with that highest delta (the **source ETF**). They're ordered by rank, then appearances, then delta. The ideal fund is the top of this list within `ranking_to` (split by style and region percentages where configured).

**Changes to the previous holdings.**

- A current holding that is still in the ideal list is kept.
- A current holding that is no longer among the best ideas at all is **sold** ("Not in best ideas top levels").
- A current holding that is still a best idea but whose rank has worsened by 5 or more (market-cap funds) or 2 or more (equal-weight funds) is **sold** ("Dropped below min ranking"). A smaller slip is tolerated – it keeps turnover down.
- Free slots are filled from the ideal list in order (**buys**).
- If no ideal holdings can be found, the previous holdings are carried over unchanged.

**Weighting.**

- **Equal**: `1 / number of holdings`.
- **Market cap**: weights are proportional to the *square root* of market cap, so a company 100× bigger gets 10× the weight rather than 100×. Each holding is then capped at **10%** and floored at **1%**, with the difference spread across the other holdings in proportion to their weights.

The result is stored in `fund_holding` (the holdings, with rank, source ETF, max delta and weight) and `fund_holding_change` (the buys and sells, with reasons, appearances and contributing ETFs).

### 12. Fund analysis snapshot

Every time a fund is recalculated, `fund_analysis` stores the full calculation behind it: for every ETF the fund could draw on, every company's market cap, whether a master was used, ETF weight, benchmark weight, delta, rank, and why it did or didn't become a best idea (`best_idea`, `delta<=0`, `delta>limit`, `beyond_top_N`, `no_market_cap`, `quarantined`). It is recomputed with the same functions as step 10 and cross-checked against the stored best ideas.

`scripts/report_fund_methodology.py` turns this into a readable export (see [Scripts](#scripts)).

### 13. Logging and notifications

- **`log`** table – status, notice and error messages from every stage.
- **`batch_run`** / **`batch_run_log`** – one row per stage run (start/end time) and the problems found during it.
- **Email** (Mailgun) – a summary to the admins when the cron completes, and an alert when a stage fails, naming the stage and the error.

---

## Simulation

The simulation replays the Wednesday logic (best ideas → fund) week by week over the holdings already in the database, to see how a fund would have been built and changed since a chosen start date. It uses the exact same best-ideas and fund code as live.

Run against development, in this order:

```bash
python scripts/sim_prep_data.py --dev    # 1. refresh stale ticker profiles, sync master tickers
python scripts/sim_benchmark.py --dev    # 2. backfill weekly historical benchmarks (edit inception_date first)
python scripts/sim_fund.py --dev         # 3. run the fund simulation (edit fund_id / inception_date / weeks first)
```

1. **`sim_prep_data.py`** – the same profile refresh and master-ticker sync as live (steps 5 and 8), so companies are grouped correctly. It asks whether to also retry tickers previously marked invalid. It's safe to re-run; it only picks up what's stale or new. Report: `.output/sim_prep_data_report.md`.
2. **`sim_benchmark.py`** – builds a benchmark snapshot for every Wednesday from `inception_date` to the latest date FMP has published data for (a few days behind today). The FMP screener has no history, so today's large-cap universe is used; each listing's **historical** market cap is then fetched and converted to USD at that date's exchange rate. Per date, each company takes its primary listing's value (never a sum of listings), companies below $10B that date are dropped, and the US/International split follows `ticker.region`. Report: `.output/sim_benchmark_report.md`.
3. **`sim_fund.py`** – for the chosen fund:
   - **erases** its existing `fund_holding`, `fund_holding_change` and `fund_analysis` rows (it refuses to run against production for this reason);
   - starting on the first Tuesday on or after `inception_date` and then every `recalc_frequency_days`, generates best ideas as of that date for all ETFs and recalculates the fund;
   - stops at the latest holdings date available (or after `weeks`, if set);
   - writes a week-by-week report of buys and sells (and holdings, with `show_holdings = True`) to `.output/sim_<fund>_<id>_<start>_to_<end>.txt`.

After a simulation, `scripts/report_fund_methodology.py` can explain any of the simulated dates.

---

## Backtesting

`python modules/bt/run.py` – a separate, older pipeline for multi-year studies, with its own database (`best_ideas_bt`) and its own object modules under `modules/bt/`. It always uses the development database server.

- Historical holdings were bought once from FactSet / Morningstar (`modules/bt/data_sources/`).
- Configuration is at the top of `modules/bt/orchestrator.py`: `START_DATE`, `END_DATE`, `CALC_PERIOD` (weekly, monthly, bi-monthly or quarterly) and which stages to run (data download, best ideas, fund construction, accounts).
- Stock prices, market caps, dividends and splits are downloaded for the whole period; style is assigned with the same classifier approach as live.
- Best ideas are computed with the `self` benchmark only (no synthetic benchmarks or master tickers in BT).
- Simulated **accounts** follow the funds day by day (trades, cash, interest, dividends) and their performance is compared to a benchmark. Annual alpha is logged and daily returns are exported to `.output/<account> - <fund>.csv`.

---

## Scripts

All scripts are run from the project root with the virtual environment active. Common conventions:

- **`--dev` / `--prod`** – which database the script uses. Default is development (or whatever `ENV_TYPE` says). The resolved environment is printed at startup.
- **`--headed`** – run the browser visibly instead of headless (scraping scripts), to watch the events and download.
- Several scripts have their parameters (`fund_id`, `provider_id`, dates…) as variables at the top of the file, marked `# <-- edit before each run`.
- All script output goes to `.output/` (git-ignored): reports and simulation results at the top level or in their own subfolder (`methodology/`), and downloaded holdings files in `.output/downloads/`.

### Live pipeline stages (run one stage by hand)

| Script | What it does |
|--------|--------------|
| `service_cron.py` | The full daily cron. Runs whatever is scheduled for today's weekday. |
| `scripts/current_categorize_tickers.py` | Sunday categorization ETF download, then the style assignment chain (step 6). |
| `scripts/current_benchmark_generator.py` | Builds today's US/Intl benchmark snapshot (step 9). Needs master tickers to exist. |
| `scripts/current_best_ideas.py` | Generates best ideas for all ETFs as of now (step 10) and prints the problems. |
| `scripts/current_model_fund.py` | Recalculates all funds as of today (steps 11–12). |

### Scraping and parsing

| Script | What it does |
|--------|--------------|
| `scripts/single_provider.py` | Scrapes one provider (`provider_id`), parses and resolves each ETF, prints the first/last rows and stores the holdings. The quickest way to test a new or broken provider (use `--headed`). |
| `scripts/single_provider_etf.py` | Same for one ETF (`provider_etf_id`); also saves the downloaded file to `.output/downloads/`. |
| `scripts/download_and_save_all_providers.py` | Runs collection for every active provider, keeps every downloaded file in `.output/downloads/<provider>/`, and writes a report with holdings / resolved / problem ticker counts per ETF to `.output/downloads/report.md`. **Clears `.output/downloads/` first** (the rest of `.output/` is left alone). |
| `scripts/debug_etf_resolve_tickers.py` | Parses a file already in `.output/downloads/` (path relative to it, e.g. `<provider>/<file>`) for a given ETF and runs ticker resolution on it, without scraping. For tuning mappings and resolution. |
| `scripts/debug_holding_duplicates.py` | Read-only report of tickers appearing on several lines of one ETF/date, split into consistent (summed) and inconsistent (quarantined) groups. Report: `.output/holding_duplicates_report.md`. |

### Ticker data maintenance

| Script | What it does |
|--------|--------------|
| `scripts/data_fill_ticker_profile.py` | Refreshes every ticker profile not checked in a week (step 5). Asks whether to retry invalid tickers too. |
| `scripts/data_fill_master_tickers.py` | Runs the master-ticker sync and the company market cap / region refresh (step 8). Report listing every group (name-matched groups in detail, worth reviewing): `.output/master_tickers_report.md`. Run `data_fill_ticker_profile.py` first – CIKs come from the profiles. |
| `scripts/data_fill_categorization_esg.py` | Fills missing style and ESG for valid companies (not share-class siblings): the style chain (CAT_ETF → PROVIDER_ETF → MODEL) for tickers with no style, then ESG for companies never fetched. Asks whether to re-scrape the categorization ETFs first, retry tickers whose factor fetch failed (ignoring the 30-day back-off), and refresh ESG for all companies. Run after `data_fill_master_tickers.py`. Report with before/after coverage: `.output/data_fill_categorization_esg_report.md`. |
| `scripts/data_fill_ticker_value_gaps.py` | Finds gaps of more than 4 weekdays in `ticker_value` since 2026-01-01 for tickers held by ETFs, and fills them from FMP history (USD converted). |
| `scripts/data_fill_ticker_value_refresh.py` | Rebuilds `ticker_value` history since 2026-01-01 from FMP's historical endpoints, **replacing** what's stored. Options: `--yes` (no confirmation), `--dry-run` (roll back), `--symbol AVGO` (one ticker). Report: `.output/data_fill_ticker_value_refresh_report.md`. |

### Simulation and reporting

| Script | What it does |
|--------|--------------|
| `scripts/sim_prep_data.py` | Simulation step 1 – see [Simulation](#simulation). |
| `scripts/sim_benchmark.py` | Simulation step 2 – historical weekly benchmarks. |
| `scripts/sim_fund.py` | Simulation step 3 – replays one fund. Development only. |
| `scripts/report_fund_methodology.py` | Read-only. For a `fund_id` and date (default: the fund's inception), writes to `.output/methodology/<fund>_<id>_<date>/`: `README.md` (strategy, constants, methodology), `etfs/<etf>_<id>.xlsx` (each ETF's holdings and active-weight calculation), `benchmark.xlsx`, `best_ideas.xlsx` (every idea and which fund filter it passed or failed) and `fund.xlsx` (holdings with their justification, and the candidates not selected). |

### Database

| Script | What it does |
|--------|--------------|
| `scripts/db_sync_dev_from_prod.py` | Copies the new `provider_etf_holding` rows from production to development (per ETF, everything newer than development's latest date), remapping tickers by symbol + exchange and adding missing tickers. Production is opened read-only. Derived data is then regenerated by running the pipeline against development. Options: `--yes`, `--dry-run`, `--provider-etf-id N`. |

---

## Environment variables

Set in a `.env` file at the project root (loaded with `python-dotenv`; it's git-ignored – ask an admin for the values). VS Code's launch configurations load the same file.

| Variable | Used for |
|----------|----------|
| `ENV_TYPE` | `development` or `production`. Chooses the database when no `--dev`/`--prod` flag is passed. Set to `production` in the Docker image. |
| `PYTHONPATH` | The project root, so `modules.*` imports resolve when running scripts. |
| `SECRET_DATABASE_HOST` / `_PORT` / `_USER` / `_PASSWORD` | Development PostgreSQL server (also used by backtesting). |
| `SECRET_DATABASE_NAME` | Development live database (`best_ideas`). |
| `SECRET_DATABASE_NAME_BT` | Backtesting database (`best_ideas_bt`), always on the development server. |
| `SECRET_DATABASE_PROD_HOST` / `_PORT` / `_USER` / `_PASSWORD` / `_NAME` | Production PostgreSQL, used with `--prod` or `ENV_TYPE=production`. |
| `SECRET_MARKET_DATA_API_KEY` | FinancialModelingPrep API key (profiles, prices, market caps, screener, ESG, factors, FX). Calls are limited to 200 per minute by the client. |
| `SECRET_MAILGUN_ENDPOINT` / `SECRET_MAILGUN_API_KEY` | Mailgun, for the admin emails. |
| `SECRET_HOLDINGS_DATA_API_KEY` | FactSet – only for the one-off backtesting historical holdings download. |
| `SECRET_TOKEN_AUTH_SIGN` | Signing secret for `modules/core/token.py` (URL tokens). Not needed by the pipelines. |

---

## Docker and hosting

The live cron runs on a hosting service from the `Dockerfile` image rather than a plain Python environment, because holdings collection needs a **real browser**:

- `pip install playwright` installs only the Python library and driver, not a browser. Chromium has to be downloaded separately, and it needs a set of operating-system libraries (graphics, fonts, NSS, etc.) that a hosting service's standard Python runtime doesn't include and doesn't let us install.
- The image starts from `python:3.13.7-slim` (Debian), installs `requirements.txt`, and then runs `playwright install --with-deps --only-shell`, which installs the headless Chromium shell **and** its system dependencies (`--with-deps` needs the root access a build step has). `--only-shell` skips the full headed browser, keeping the image smaller.
- `ENV_TYPE=production` is set in the image, so the container uses the production database unless told otherwise.
- The image has no `CMD`; the command (`python service_cron.py`) and its daily schedule are set in the hosting service's cron job configuration, where the `SECRET_*` environment variables are also defined.

To build and test the image locally:

```bash
docker build -t best-ideas .
docker run --env-file .env best-ideas python service_cron.py
```

Inside the container `localhost` is the container itself, so the database host in the env file must be reachable from Docker (e.g. `host.docker.internal` for a database on your machine).

---

## Python

### Virtual environment

Create a virtual environment at the command prompt with:
`python -m venv .venv`

Activate it with:
`.venv/Scripts/activate` (Windows) or `source .venv/bin/activate` (Linux/macOS)

Install the dependencies:
`pip install -r requirements.txt`

Playwright also needs its browser once per machine:
`playwright install chromium`

#### Reset the virtual environment
- At the terminal prompt: `deactivate`
- Delete the `.venv` directory
- Run at the terminal: `python -m venv .venv`
- Re-install the dependencies: `pip install -r requirements.txt` (or the module list below)
- Generate a new requirements.txt

#### Tell VS Code to use the venv's Python interpreter
- Press `Ctrl+Shift+P`
- Type **Python: Select Interpreter**
- Choose `.venv\Scripts\python.exe` (Windows)

### Modules

#### In use
`python-dotenv psycopg psycopg-pool psycopg-binary pydantic pandas openpyxl xlrd playwright playwright-stealth mailgun scikit-learn httpx tld bcrypt`

| Module | Used for |
|--------|----------|
| `python-dotenv` | Loading `.env` |
| `psycopg`, `psycopg-pool`, `psycopg-binary` | PostgreSQL access and connection pools |
| `pydantic` | Parsing JSON configuration (fund strategies, provider mappings) |
| `pandas`, `openpyxl`, `xlrd` | Reading holdings files (`.xlsx`, `.xls`, CSV), calculations and Excel exports |
| `playwright`, `playwright-stealth` | Browser automation for scraping |
| `mailgun` | Admin emails |
| `scikit-learn` | Value/growth style classifier (live and backtesting) |
| `httpx`, `tld`, `bcrypt` | Web/domain utilities and hashing (`modules/core/util.py`) |

##### FactSet modules
Used only once, for the backtesting historical holdings download:
`fds.sdk.utils fds.sdk.FactSetOwnership fds.sdk.Formula`

#### Package manager
Packages are installed with `pip`, for example:
`pip install python-dotenv`

Update `requirements.txt` with:
`pip freeze > requirements.txt`

To see a library's dependencies: `pip show library-name`

---

## Database

### Single source of truth

The database is the single source of truth: configuration (providers, ETFs, mappings, funds and strategies), collected data and every result live there. We use the [psycopg](https://www.psycopg.org/) library for connection pool management and CRUD actions, and the [psycopg.rows](https://www.psycopg.org/psycopg3/docs/advanced/rows.html) utility to return rows as classes. Classes are created with the [dataclass](https://www.datacamp.com/tutorial/python-data-classes) decorator – one module per table under `modules/object/` (live) and `modules/bt/object/` (backtesting).

The authoritative schema is `modules/object/_db_schema.sql` (live) and `modules/bt/object/_bt_db_schema.sql` (backtesting).

### Databases

| Database | Server | Used by |
|----------|--------|---------|
| `best_ideas` | development (local) | live scripts and simulation by default |
| production database | production (Render) | the hosted cron, and scripts run with `--prod` |
| `best_ideas_bt` | development (local) | backtesting only |

### Main tables (live)

| Table | Contents |
|-------|----------|
| `provider`, `provider_etf` | Scraping configuration and ETF metadata |
| `provider_etf_holding` | Downloaded holdings, one row per line per date |
| `ticker` | Companies/listings: identifiers, sector, country, style, ESG, invalid reason, master ticker, combined market cap |
| `ticker_value` | Daily validated price and market cap (USD) |
| `categorize_etf`, `categorize_etf_holding`, `categorize_ticker` | Style reference ETFs, their holdings and constituents with factors |
| `benchmark`, `benchmark_holding` | Synthetic benchmarks and their weekly weights |
| `best_idea` | Top 10 active-weight ideas per ETF, date and benchmark mode |
| `fund`, `fund_holding`, `fund_holding_change`, `fund_analysis` | Model funds, their holdings, buys/sells and the calculation snapshot |
| `batch_run`, `batch_run_log`, `log` | Run history, problems and log messages |

### Copy production data to development

**Holdings only (routine):** `python scripts/db_sync_dev_from_prod.py` copies the new holdings from production and remaps tickers; then run the pipeline stages you need against development.

**Full data copy** – in a CMD window and pgAdmin:
1. Create a data-only backup of the production database in a CMD window:
    > pg_dump -h dpg-d5do2kje5dus739gfud0-a.virginia-postgres.render.com -U admin -d best_ideas_eq6y --column-inserts --disable-triggers --data-only -f C:\Users\Yuval\Downloads\db_backup_data_only.sql
    - You will need the admin password – get it from render.com
    - pg_dump warns about circular foreign-key constraints on `ticker` (the self-referencing `master_ticker_id`). This is expected: `--disable-triggers` handles it, provided the restore runs as a superuser (step 3)
2. Truncate all the tables in the development database in pgAdmin:
    > call truncate_all_tables();
3. Back in the CMD window, restore the database **as the `postgres` superuser**:
    > psql -h localhost -p 5432 -U postgres -d best_ideas -f C:\Users\Yuval\Downloads\db_backup_data_only.sql
    - You will need the postgres password (set when PostgreSQL was installed locally)
    - Don't restore as `admin`: it isn't a superuser, so the dump's `DISABLE TRIGGER ALL` statements fail and `ticker` rows that reference a master row not yet inserted are rejected with `violates foreign key constraint "ticker_master_ticker_id_fkey"`

---

## Playwright

We use [Playwright](https://playwright.dev/python/) to drive a Chromium browser like a user would, so we can download holdings files from provider websites. It runs [headless](https://playwright.dev/python/docs/browsers) by default; pass `--headed` to any script to watch the browser perform the events and the download.

To reach as many pages as possible, each provider (domain level) and, if needed, each ETF can be configured with:
1. **wait_pre_events / wait_post_events** – a selector in the page that Playwright waits for to be visible before / after the events.
2. **events** – a series of recorded steps to run, usually cookie acceptance and investor-type identification. After these, the page normally shows the desired content.
3. **trigger_download** – the element to click that starts the file download.
4. **mapping** – how to read the downloaded file, including where to find the holdings date (in the file, the file name, or on the page).

#### What are "selectors"
A selector is a combination of the tag type and an attribute value, for example:
- `div.content` – a `div` tag with the class "content"
- `section#list-of-items` – a `section` tag with the id "list-of-items"

### Event recording
We use the [Playwright CRX Chrome plugin](https://chromewebstore.google.com/detail/jambeljnbnfbkcpnoiaedcabbgmnnlcd) to record the events. Copy the recorded JSONL (JSON Lines) into the database `events` column and make it a JSON array. The events are played back on request by the dispatcher in `modules/parse/url.py`.

Events that can't be recorded can be added by hand after recording:
- `mouse` – scroll in x/y
- `scroll_to_first` – scroll to the first instance of a selector

---

## Claude Code and the database MCP servers

Claude Code can query the databases directly through [MCP](https://modelcontextprotocol.io/) (Model Context Protocol) servers, which is useful for investigating data while working on the code. Three servers are used:

| Server | Database | Access | Implementation |
|--------|----------|--------|----------------|
| `postgres` | development `best_ideas` | read and write | `mcp/mcp-postgres-rw` (local, in this repo) |
| `postgres-bt` | `best_ideas_bt` | read and write | `mcp/mcp-postgres-rw` |
| `postgres-prod` | production | **read-only** | `@modelcontextprotocol/server-postgres` (official, via `npx`) |

`mcp/mcp-postgres-rw/index.mjs` is a small MCP server exposing one `query` tool that runs any SQL against the connection string it's given. The official server is read-only, so production goes through it deliberately.

### Setup

1. Install [Node.js](https://nodejs.org/) (LTS).
2. Install the local server's dependencies:
   ```bash
   cd mcp/mcp-postgres-rw
   npm install
   ```
3. Create `.mcp.json` in the project root (it's git-ignored because it contains passwords – never commit it):
   ```json
   {
     "mcpServers": {
       "postgres": {
         "command": "node",
         "args": ["mcp/mcp-postgres-rw/index.mjs", "postgresql://<user>:<password>@localhost:5432/best_ideas"]
       },
       "postgres-bt": {
         "command": "node",
         "args": ["mcp/mcp-postgres-rw/index.mjs", "postgresql://<user>:<password>@localhost:5432/best_ideas_bt"]
       },
       "postgres-prod": {
         "command": "npx.cmd",
         "args": ["-y", "@modelcontextprotocol/server-postgres",
                  "postgresql://<user>:<password>@<prod-host>:5432/<prod-db>?sslmode=require"]
       }
     }
   }
   ```
   Use `npx` instead of `npx.cmd` on Linux/macOS. The connection details are the same as the `SECRET_DATABASE_*` values in `.env`.
4. Approve the servers: Claude Code asks the first time it finds a project `.mcp.json`. To approve them automatically, add `"enableAllProjectMcpServers": true` to `.claude/settings.json` (also git-ignored).
5. Restart Claude Code (or reload the VS Code window) and run `/mcp` to check that all three servers are connected.

The tools then appear to Claude as `mcp__postgres__query`, `mcp__postgres-bt__query` and `mcp__postgres-prod__query`. To skip the permission prompt for queries, add them to `permissions.allow` in `.claude/settings.local.json` – keep the prompt for the read/write servers if you want to review statements that change data.

Project guidance for Claude (architecture, commands, conventions) is in `CLAUDE.md`.
