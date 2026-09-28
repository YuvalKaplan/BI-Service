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
  - [3. Universe screener](#3-universe-screener)
  - [4. Ticker maintenance](#4-ticker-maintenance)
  - [5. Large-cap universe](#5-large-cap-universe)
  - [6. Benchmark generation](#6-benchmark-generation)
  - [7. Best ideas](#7-best-ideas)
  - [8. Fund holdings formation](#8-fund-holdings-formation)
  - [9. Fund analysis snapshot](#9-fund-analysis-snapshot)
  - [10. Logging and notifications](#10-logging-and-notifications)
- [Ticker utilities](#ticker-utilities)
  - [Registration](#registration)
  - [Profile refresh](#profile-refresh)
  - [Prices and market caps](#prices-and-market-caps)
  - [Share-class consolidation (master tickers)](#share-class-consolidation-master-tickers)
  - [Style (value / growth)](#style-value--growth)
  - [ESG qualification](#esg-qualification)
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
FMP screener ──► screener_listing ──────────────────────────────► ticker, ticker_value
                          │                                             │  ▲
                          │                                             ▼  │  master sync: listings → companies
                          ▼                                             │
                  universe_company ◄────────────────────────────────────┤
                          │                                             │
                          ▼                                             │
                  benchmark_holding ──────────► best_idea ◄─────────────┘
                                                    │
                                                    ▼
                                    fund_holding, fund_holding_change, fund_analysis
```

### Weekly schedule

`service_cron.py` runs once a day. What it does depends on the weekday (UTC):

| Day | Steps |
|-----|-------|
| Tue – Sat | Holdings collection (step 2) → universe screener (step 3) → ticker maintenance (step 4): profiles → values → companies → style |
| Sun | Ticker maintenance, weekly part (step 4): categorization ETFs → ESG |
| Wed | *after the Tue–Sat steps, whose screener also stores the screen that day:* large-cap universe (step 5) → benchmark generation (step 6) → best ideas (step 7) → fund updates (steps 8–9) |
| Mon | Nothing |

Collection runs Tue–Sat because providers generally publish the previous trading day's holdings, so those runs pick up Monday to Friday. Every day, listings come in first – the holdings and the large-cap screener – and then the ticker maintenance goes over every ticker (profiles, values, companies, style – the [ticker utilities](#ticker-utilities)), so the Wednesday generators only use maintained tickers. Each step reads what the previous one stored, so any of them can be re-run on its own: on Wednesday the universe is built from the stored screen and the linked companies, and the benchmarks are formed on the same day the best ideas and funds consume them. If a step fails the cron stops and emails the admin: in particular the FMP screener is retried 3 times and then fails the run, and an empty universe or benchmark (no market cap could be validated) fails it too, so best ideas and funds never run on a partial or missing benchmark.

When the run finishes (or fails), an email summary goes to the admins (step 10).

### 1. Providers and ETFs

Everything that drives collection is configuration in the database, not code:

- **`provider`** – a fund manager's website (e.g. JP Morgan, Capital Group): start URL, default file format and column mapping, and any browser events needed to reach the download (cookie banners, "I am an investor" prompts…).
- **`provider_etf`** – one active ETF of that provider: its own URL/events/mapping when they differ from the provider's, `region` (`US` / `International`), `style_type` and `cap_type` as described by the manager, and an optional `benchmark_id` pointing at one of the synthetic benchmarks (step 6).
- **`fund`** – a model fund we build, with its `strategy` stored as JSON (step 8).

Disabling a provider or ETF in the database removes it from every stage.

### 2. Holdings collection

`modules/cron/etf_downloader.py` → `modules/parse/download.py`

1. All active providers are processed in parallel (5 at a time).
2. For each provider, a headless Chromium browser (Playwright with stealth settings to look like a normal Chrome) opens the provider page, replays the recorded events, and triggers each ETF's holdings download. See [Playwright](#playwright) for how pages and events are configured. A failed scrape is retried up to 3 times.
3. The downloaded file (Excel `.xls`/`.xlsx` or CSV) is parsed with the ETF's **mapping** (`modules/parse/convert.py`): which sheet, which header row, which columns hold ticker / ISIN / name / shares / market value / weight, and where the holdings date is (in the file, the file name, or on the web page).
4. Each line is resolved to (or registered as) a row in the `ticker` table – [Registration](#registration). Lines that can't be resolved are counted as "problem tickers" and dropped. Their prices and market caps are stored afterwards, once per ticker, by the ticker maintenance (step 4).
5. The resolved lines are stored in `provider_etf_holding` and the ETF's `last_downloaded` is stamped.

The admin email lists, per provider, how many of its ETFs were downloaded.

### 3. Universe screener

`modules/cron/universe_screener.py` – Tue–Sat, after holdings collection

The investable large-cap universe (company market cap ≥ **$10B in USD**) comes from the FMP company screener. Every day its listings are registered in the `ticker` table alongside the ETF holdings' (step 2), so the ticker maintenance (step 4) covers everything the Wednesday generators use; on Wednesdays the screen is also stored for the large-cap universe (step 5).

1. The FMP company screener is paged through once per exchange: NYSE, NASDAQ, AMEX, TSX, LSE and 35 other international exchanges (Japan, Germany, Hong Kong, Australia, Switzerland, France, China, India, Taiwan, Korea, the Nordics, …). FMP compares `marketCapMoreThan` against each listing's **local-currency** market cap, so the $10B threshold is first converted into the exchange's currency at the latest FX rate (an exchange with no known currency is skipped). The log shows each exchange's local threshold and smallest company returned, in USD.
2. The screener returns **listings, not companies**, with the whole company's cap on most of them – foreign lines, depositary receipts, even preferred shares and notes. A company should enter through its **home-market ordinary listing**, so every line is classified:
   - preferred, note, warrant, unit and participation-certificate lines are skipped (Bank of America's cap on a Merrill Lynch note, Corteva's on an EIDP preferred);
   - LSE International Order Book lines (`0Q16`, mirrors of foreign securities carrying the issuer's home-market data) are skipped;
   - `home` – a line in the domicile's home market (as ranked in [Share-class consolidation](#share-class-consolidation-master-tickers)), on a US exchange, or anywhere for a company domiciled where no exchange is screened (Bermuda, Cayman, Hungary, …);
   - `foreign` – any other line (Exxon on XETRA, Cisco's Canadian DR, Toyota mirrored on LSE), decided in step 5.
3. Each home-market line is registered in the `ticker` table ([Registration](#registration)) – a new one gets its FMP profile at once (currency, ISIN, CIK, so the master sync can link it the same day). On Tuesday, Thursday, Friday and Saturday that's all – about a minute.
4. **Wednesdays** (and when run by hand), every line is also stored in `screener_listing` with its type and quote (market cap and price, in local currency), dated at the latest completed trading day – Tuesday's close for the Wednesday run. The ticker maintenance (step 4) then values the screen's registered lines along with the ETF holdings' tickers, with the quote's share count as the glitch filter's reference; about 900 of the ~2,480 home-market lines aren't held by any ETF, which is why the screen is valued weekly. Foreign lines are only stored: about 490 a week, nearly all of them other listings of companies already in.

### 4. Ticker maintenance

`service_cron.py`, with the [ticker utilities](#ticker-utilities) – every day but Monday

The holdings (step 2) and the screener (step 3) register the tickers they read; this step keeps every ticker correct and current by running the ticker utilities over all of them:

- **Tue–Sat**, after the screener: [Profile refresh](#profile-refresh) → [Prices and market caps](#prices-and-market-caps) (every ticker in an ETF's recent holdings, plus the stored screen's lines on Wednesdays) → [Share-class consolidation](#share-class-consolidation-master-tickers) → [Style](#style-value--growth) assignment for companies without one.
- **Sunday**: the categorization ETFs are downloaded (`modules/cron/categorize_downloader.py` – the reference data of [Style](#style-value--growth)), then every company's [ESG](#esg-qualification) data is refreshed.

So by the time the Wednesday steps run, every listing they can use has a current profile, a validated value for the day, its company and a style.

### 5. Large-cap universe

`modules/cron/universe_builder.py` – Wednesdays, after the Tue–Sat steps

Turns the stored screen (the latest on or before today) into one row per company, using the companies as the master sync left them (step 4, [Share-class consolidation](#share-class-consolidation-master-tickers)):

1. **Home-market lines**, each at its latest validated market cap on the screen date or up to 5 days before – a market closed that day keeps its last close (all of JPX on a Japanese holiday). A listing without one (withheld all week) or flagged invalid is left out.
2. **Foreign lines** are admitted only when they duplicate no company already in – not a listing of one, no shared ISIN, no matching company name of the same domicile – and are quoted in their exchange's currency. What's left is a company listed only outside its domicile (dsm-firmenich in Amsterdam, Prada in Hong Kong); those lines are registered as in step 3 and valued as in [Prices and market caps](#prices-and-market-caps).
3. **One row per company** – represented by its master, at its company market cap (`ticker.company_market_cap`, else the listing's own value), with its `ticker.region`.
4. **Duplicate guard** – drops any company the sync failed to merge (same evidence as [Share-class consolidation](#share-class-consolidation-master-tickers), plus a matching name for a company not anchored in its home market), keeping home-market companies first, across both regions.

The result is stored in `universe_company` for the screen date. No market-cap floor is applied here – each benchmark applies its own (step 6). Every skip and drop is logged and counted in the admin email; the sim prep report lists them.

### 6. Benchmark generation

`modules/cron/benchmark_generator.py` – Wednesdays, after the universe

Forms every enabled benchmark of the `benchmark` table from the latest stored universe. Each row specifies which companies it holds:

| Benchmark | `region` | `cap_type` | `style_type` | `market_cap_min` |
|-----------|----------|------------|--------------|------------------|
| US Large Cap Blend | `US` | `large` | `blend` | $10B |
| Intl Large Cap Blend | `International` | `large` | `blend` | $10B |

A benchmark holds the universe's companies of its `region` with a company market cap of at least `market_cap_min` (USD) and of its `style_type` (`blend`/`core`: any style; `value`/`growth`: the company's `ticker.style_type`). Each is weighted by market cap (`weight = market cap / total`) and the snapshot is stored in `benchmark_holding`, dated at the universe's screen date – Tuesday's close for the Wednesday run (a re-run replaces it). The universe only holds companies screened at $10B and up, so a benchmark with a lower `market_cap_min` would be incomplete – a notice is logged. An empty benchmark fails the run.

### 7. Best ideas

`modules/cron/best_ideas_generator.py` – Wednesdays

A manager's best ideas are the stocks they hold at a **higher weight than the market would**. For each active ETF:

1. **Holdings** – the latest holdings downloaded in the last 7 days, with duplicate lines summed or quarantined ([Registration](#registration)).
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
   | `full_universe` | the company's weight in the ETF's linked benchmark (step 6); 0 if not in it | only when the ETF has a `benchmark_id` |

5. **Selection** – companies with a positive delta, ranked from highest delta down. A delta above **20%** is treated as abnormal and dropped. The top **10** per ETF are stored, with their rank.

Problems (no recent download, stale holdings, quarantined duplicates, insufficient coverage, errors) are recorded in `batch_run_log` and listed in the admin email.

### 8. Fund holdings formation

`modules/cron/funds_update.py`, `modules/calc/model_fund.py` – Wednesdays, after best ideas

Each fund is defined by a JSON **strategy**, for example:

```json
{
  "holdings": 30,
  "allocation": "market_cap",
  "benchmark": "full_universe",
  "cap":    { "name": "large" },
  "style":  { "name": "blend", "value": 50, "growth": 50 },
  "region": { "name": "Global", "split": { "US": 70, "International": 30 } },
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
| `region` | `US`, `International`, or a `split` with `US`/`International` percentages (exactly these keys — any other key is rejected); omitted = no filter |
| `provider_etfs` | limit to these ETFs' ideas (empty = all ETFs) |
| `exchanges` | limit to stocks on these exchanges |
| `esg_only` | only ESG-qualified companies |
| `ranking_from` / `ranking_to` | which ranks of each ETF's best ideas count (e.g. 1–3 = each manager's top three) |
| `recalc_frequency_days` | the fund is skipped until this many days have passed since its last recalculation |

**Building the candidate list.** All stored best ideas (latest per ETF) are filtered by the strategy:

- **Region** – `US` takes ideas from US ETFs in companies whose `ticker.region` is `US`; `International` takes ideas from international ETFs in companies whose region is `International` (region = primary listing, see [Share-class consolidation](#share-class-consolidation-master-tickers)). With a split, the international list is built first and those companies are excluded from the US list.
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

### 9. Fund analysis snapshot

Every time a fund is recalculated, `fund_analysis` stores the full calculation behind it: for every ETF the fund could draw on, every company's market cap, whether a master was used, ETF weight, benchmark weight, delta, rank, and why it did or didn't become a best idea (`best_idea`, `delta<=0`, `delta>limit`, `beyond_top_N`, `no_market_cap`, `quarantined`). It is recomputed with the same functions as step 7 and cross-checked against the stored best ideas.

`scripts/report_fund_methodology.py` turns this into a readable export (see [Scripts](#scripts)).

### 10. Logging and notifications

- **`log`** table – status, notice and error messages from every stage.
- **`batch_run`** / **`batch_run_log`** – one row per stage run (start/end time) and the problems found during it.
- **Email** (Mailgun) – a summary to the admins when the cron completes, and an alert when a stage fails, naming the stage and the error.

---

## Ticker utilities

`modules/ticker/` – the stages a ticker goes through. The holdings (step 2) and the large-cap screener (step 3) bring listings in; these utilities register them, fix what's wrong with them and keep them current, so every later step works from correct company data. The pipeline steps use them – mostly the ticker maintenance (step 4) – and so do the simulation and the maintenance scripts.

They usually run in this order, each building on the one before:

1. [Registration](#registration) – as the holdings and the screen are read: which ticker a line is.
2. [Profile refresh](#profile-refresh) – identifiers, currency and validity, so the values are taken in the right currency and skip tickers that are no longer valid.
3. [Prices and market caps](#prices-and-market-caps) – one validated value per ticker in use for the day.
4. [Share-class consolidation](#share-class-consolidation-master-tickers) – which listings are one company, and its market cap and region (from the values).
5. [Style](#style-value--growth) – value or growth, per company (after the grouping, so a new listing isn't classified on its own).
6. [ESG](#esg-qualification) – weekly, per company.

| Utility | Module | Used by |
|---------|--------|---------|
| Registration | `resolver.py` (holdings), `modules/cron/universe_screener.py::register_listings` (screen) | steps 2, 3 and 5 (foreign lines) |
| Profile refresh | `refresh.py` | step 4, `sim_prep_data.py`, `data_fill_ticker_profile.py` |
| Prices and market caps | `valuation.py` (which tickers, once each), `pricing.py` (validation, glitches, USD) | step 4, step 5 (foreign lines), `sim_prep_data.py`, `current_ticker_values.py`, `single_provider*.py` |
| Share-class consolidation | `master.py`, `company.py` (primary listing), `identity.py` (same-company evidence) | step 4, `sim_prep_data.py`, `data_fill_master_tickers.py` |
| Style | `style.py`, `modules/calc/classification.py` | step 4, `current_categorize_tickers.py` |
| ESG | `esg.py`, `modules/calc/esg.py` | step 4 (Sunday), registration of a new ticker |

### Registration

`modules/ticker/resolver.py` (holding lines), `modules/cron/universe_screener.py::register_listings` (screener lines)

Each holding line is matched to a company using the FinancialModelingPrep (FMP) API:

- **US ETFs** – by symbol, via the FMP company profile.
- **Non-US ETFs** – by **ISIN** when the file has one. Without an ISIN, by FMP symbol search, accepting a candidate only when its company name matches the holding's name; failing that, by FMP name search, again only with a verified name match.

  A name match requires every meaningful word of the shorter name to match the longer one and cover more than half of it (legal suffixes such as *Inc*, *PLC*, *Holdings* are ignored). A single shared word is never enough – loose matching would otherwise map unrelated holdings onto the same ticker.

When a profile is found, the ticker is inserted or updated with its identifiers (ISIN, CUSIP, CIK), name, exchange, sector, industry, country and currency. It's **marked invalid** (and ignored from then on) when it is crypto, has no company name or market cap, or its name identifies it as a fund, ETF, trust or index. New tickers get their ESG data straight away ([ESG](#esg-qualification)).

Results are cached for the duration of a provider's run, so a stock held by many of its ETFs costs one lookup.

The screener registers its lines by exchange and symbol instead (`universe_screener.register_listings`): a listing already known is used as it is, a new one is added with its FMP profile. Registration stores no value – see [Prices and market caps](#prices-and-market-caps).

**Duplicate lines.** A ticker can still appear on several lines of one ETF on one date. When best ideas are computed, those lines are summed if they all imply the same share price (within 2%) – they are lots of the same security. If the prices disagree, they are most likely different securities that resolved to the same ticker, so all of that ticker's lines are **quarantined** (excluded) and reported.

### Profile refresh

`modules/ticker/refresh.py::refresh_ticker_profiles`

Every day of the ticker maintenance (Tue–Sat), every ticker whose profile hasn't been checked for a week is refreshed from FMP (ISIN, CUSIP, CIK, name, sector, industry, country, currency, actively trading). A ticker is marked invalid if its profile can't be fetched, turns out to be crypto or a fund/ETF, or is no longer actively trading. A ticker that checks out again has its invalid flag cleared. When a ticker's currency changes (or becomes known and differs from its exchange's), its `ticker_value` history since 2026-01-01 is rewritten from FMP converted from the new currency – otherwise every new value would disagree with the stored ones and the ticker would end up marked invalid. Likewise, when any stored market cap is 2.5-fold off what the profile's share count gives at that day's price (a history on a wrong share count throughout has no jump for the glitch filter to catch), the history is rewritten against the profile's share count.

### Prices and market caps

`modules/ticker/valuation.py` (which tickers, once each, in parallel), `modules/ticker/pricing.py` (validation, glitches, USD)

Once a day, in the ticker maintenance, every ticker in use gets its price and market cap for the latest completed trading day stored in `ticker_value` (`pricing.latest_value_date`: before 17:00 New York time that is the previous day, and a weekend steps back to Friday – the date every step stores and reads values under): the valid tickers in each active ETF's latest holdings from the last 7 days (the holdings best ideas can use; about 3,600) and, on Wednesdays, the registered lines of the stored screen (about 900 more). Each ticker is valued once however many ETFs hold it, 5 in parallel (FMP's rate limit of 200 calls a minute is the bound), and one already valued for the date is skipped. The universe builder values the few foreign lines it admits the same way, and `scripts/single_provider*.py` the tickers they just stored.

FMP's profile endpoint occasionally returns wrong values, so the numbers are taken from FMP's **historical** price and market-cap endpoints and **validated**: the last 21 days fetched now are compared against what we stored before. If any overlapping day differs by more than 0.5% (price) or 1% (market cap), today's value is withheld. If a ticker has gone 5 days without a good value, it's marked invalid with the mismatch details.

FMP's market-cap **history itself has glitches** – a wrong unit or share count for a day or for months: Compass at 1/100 (pence vs pounds) for two weeks in June 2026, a NYSE line at ×266 every weekend, and a late-July share-count change that put Hyundai Glovis at ×7.7 and Prudential's Hong Kong line at ×10 while fixing Centrica's, SPIE's and Hanwha Ocean's older history. Every history fetch (validation, rebuilds, the sim) therefore cleans the market caps first (`pricing.clean_market_caps`), on a year of context: a market cap doesn't move 3-fold in a day unless its price does (a crash moves both – EyePoint's -67% day is kept), so such jumps split the series into runs. A spike or streak that comes back is a glitch; for a jump that stays, FMP's **current quote** of the listing (market cap ÷ price = its share count; from the screener row, else the profile) decides which side is wrong – values whose implied share count is 2.5-fold off it are the glitch. The quote itself is ignored when the only values it agrees with are a spike or streak that came back (SABESP's profile carries the same wrong share count as an eight-day glitch in August), or when it disagrees with an entire established history; against a new line it wins (Uniper's new XETRA line: every value on 8.7B shares, the quote 416M). A glitch is **repaired** as that day's price × the share count of the nearest good values (or the quote's), so the value stays fresh; when FMP scaled the price as well (≥ 20-fold, pence vs pounds) it is dropped. The profile is only fetched for a series that shows a glitch, a 2-fold move the price didn't make (DuPont's share count went 410M → 137M: exactly 3×), or is new; screened listings use the screener's quote at no cost.

All market caps are converted to **USD** using the historical exchange rate for that same date, from the currency FMP reports the listing in (`ticker.currency`; a market doesn't always report in its exchange's currency – Compass on LSE and Jardine Matheson on Singapore report in USD, Hong Kong's RMB counters in CNY), else the currency of the exchange the stock trades on.

### Share-class consolidation (master tickers)

`modules/ticker/master.py::sync_masters_and_company_data`, with `company.py` (primary listing) and `identity.py` (same-company evidence)

Some companies trade as more than one listing (Alphabet as `GOOGL` and `GOOG`, and on XETRA as `ABEA`; Samsung on KSC, LSE and Vienna). Left alone they would be weighted twice and could both be picked by a fund. They are grouped instead – every ticker in the database, from the ETF holdings (step 2) and the screener (step 3) alike. It runs every day of the ticker maintenance, after the profile refresh and the values, so listings first seen that day are linked at once, from up-to-date CIKs, ISINs and names, and company caps come from the day's values; it only reads and writes the database (no FMP calls):

- Tickers sharing an SEC **CIK** form a group. A ticker with no CIK (typically a foreign listing) whose normalized name matches exactly one CIK company joins that company – this is how a US company's German or London listings end up under its US master. Other no-CIK tickers are grouped by identical normalized name: case, accents, punctuation and spacing ignored ("SK Telecom Co.,Ltd" = "SK Telecom Co., Ltd."), legal suffixes such as Corp/Ltd *not* stripped – that could merge unrelated companies.
- Companies are then merged on **same-company evidence** (`modules/ticker/identity.py`), because FMP's listings of one company often carry different names (a rename applied to one listing only: "GE Aerospace" / "General Electric Company"), different ISINs (depositary receipts) and no CIK – and FMP reports the whole company's market cap on each, so a missed listing counts the company twice:
  - a shared **ISIN**, when the names agree on their first meaningful word ("Toyota Motor Corp." / "Toyota Motor Corporation") or the market caps agree within 10% ("Exxon Mobil" / "Exxonmobil Holdings") – FMP occasionally attaches another company's ISIN to a listing (Seabridge Gold carrying Santander's), whose cap is nowhere near; rejected ISIN matches (renames, typos, wrong ISINs) are listed in the master-groups report for review;
  - matching names of the same domicile with market caps within 2% on the same date (Midea's Shanghai and Hong Kong shares, "AXIA Energia" / "AXIA Energia S.A.");
  - a **depositary receipt** whose company name matches (Cisco's Canadian DR, Ping An's Singapore DR);
  - two different CIKs are never merged, except a dual-listed company sharing an ISIN under the identical name (Rio Tinto plc / Ltd).
- Each group's **master** is its **primary listing** (below): a new group elects it, and an existing group whose master isn't its primary listing is moved to it (`align_masters_to_primary` – e.g. Bank of America from `0Q16`, its LSE order-book line, to `BAC`). The current master wins ties and a listing needs a market cap in the last 90 days to take over, so masters don't swap back and forth; market-cap moves never change a master. Every other member points at it through `ticker.master_ticker_id`. Ids stored under an earlier master (benchmark snapshots, fund holdings) are resolved to the current master when read.
- A sibling that's no longer tied to its company – by CIK, by ISIN (names agreeing or same domicile), by identical name, or (same domicile) by matching or depositary-receipt names, to the master or any other listing of the group – is unlinked first (e.g. after a profile correction), so it can be regrouped in the same run; a CIK that differs from the master's unlinks unless the company is dual-listed. Chained or circular links are flattened.
- **Primary listing** – listings are ranked: active ones first (a market cap in the last 30 days, so an old ticker left behind by a ticker change drops out), ordinary shares before preferred, note, depositary, when-issued and unit lines (including Korean preferred codes, which don't end in 0), then by market – the domicile country's exchanges (HK counts as home for Chinese companies), then for Dutch and Luxembourg holding companies the other continental exchanges they typically list on (Airbus and Euronext in Paris, Stellantis and Tenaris in Milan, argenx in Brussels), then a US listing (for US-listed, foreign-domiciled companies such as Eaton or Medtronic), then other countries' exchanges, then OTC and LSE's International Order Book – then lines quoted in their exchange's own currency before others (Tencent's HKD line before its RMB counter), then the master. The first is the primary listing.
- `ticker.company_market_cap` (master only) is the primary listing's latest market cap. FMP reports the **whole company's** market cap on every listing, so listings are never summed. Best ideas and funds measure a company **as of the holdings date** instead: the first listing in the same ranking with a value near that date, so historical (sim) dates use that date's values.
- `ticker.region` (every listing) is the company's region: `US` when the primary listing trades on NYSE/NASDAQ/AMEX (or, for a US company, on OTC), else `International`. TSM, ASML, SAP, Shopify, STMicroelectronics and ArcelorMittal are International; Eaton, Medtronic, Linde and ARM (an ADR with no UK listing) are US.

Benchmarks, best ideas and funds all work at company level: a sibling is replaced by its master, the company market cap is used instead of a single listing's, and US/International is decided by `ticker.region`.

### Style (value / growth)

Funds can be restricted to value or growth stocks, so each company needs a `style_type`.

**Reference data (Sundays)** – `modules/cron/categorize_downloader.py`

A list of index-style ETFs with a known style and cap size (e.g. a large-cap value index ETF) is kept in `categorize_etf`. Their holdings are scraped the same way as provider ETFs, and every constituent is stored in `categorize_ticker` with that ETF's style and cap type, plus a set of fundamental factors from FMP (growth rates, margins, valuation ratios, yields).

**Assigning style to tickers (Tue–Sat, after the master sync)** – `modules/ticker/style.py`, in order of preference, only for tickers with no style yet (the `type_from` column records the source):

1. `CAT_ETF` – the ticker is a constituent of a categorization ETF (same symbol and exchange): take its style and cap type.
2. `PROVIDER_ETF` – the ticker is held by a provider ETF that describes itself as value or growth: take that style.
3. `MODEL` – a gradient-boosting classifier (`modules/calc/classification.py`) is trained on the categorized constituents' factors (market cap, sector, industry, growth rates, margins, P/E, P/B, yields…) and predicts value or growth for the rest. Tickers without factor data are retried after 30 days.

Style is a company-level attribute: only master and standalone tickers are classified (share-class siblings use their master's style – see [Share-class consolidation](#share-class-consolidation-master-tickers)). That's why it runs after the master sync: a listing first seen that day is grouped with its company before it could be classified on its own.

### ESG qualification

`modules/ticker/esg.py`, `modules/calc/esg.py`

On Sundays (and immediately for any new ticker), the FMP ESG disclosure and ESG risk rating of every valid company are fetched. A company is `esg_qualified` when every factor that is available passes:

| Factor | Passing |
|--------|---------|
| ESG risk rating | `AAA`, `AA`, `A`, `BBB`, `BB` or `B` |
| ESG score | ≥ 50 |
| Governance score | ≥ 50 |

A company with no ESG data at all is not qualified. The raw factors are stored alongside the flag. Funds with `esg_only` only pick qualified companies.

---

## Simulation

The simulation replays the Wednesday logic (best ideas → fund) week by week over the holdings already in the database, to see how a fund would have been built and changed since a chosen start date. It uses the exact same best-ideas and fund code as live.

Run against development, in this order:

```bash
python scripts/sim_prep_data.py --dev    # 1. screen, refresh profiles, value, sync master tickers, build the universe
python scripts/sim_benchmark.py --dev    # 2. backfill weekly historical benchmarks (edit inception_date first)
python scripts/sim_fund.py --dev         # 3. run the fund simulation (edit fund_id / inception_date / weeks first)
```

1. **`sim_prep_data.py`** – the same steps as live, in the same order: universe screener (step 3, storing the screen), the ticker maintenance's profile refresh, values and master-ticker sync (step 4), and the large-cap universe (step 5). The screen is dated at the latest date FMP has published data for (a few days behind today) rather than the latest trading day, and a listing whose value can't be validated on that date stays in the universe (the backfill fetches its whole history anyway). It asks whether to also retry tickers previously marked invalid. It's safe to re-run; it only picks up what's stale or new. Report: `.output/sim_prep_data_report.md` – including the duplicate companies dropped and the foreign / non-equity lines skipped or admitted.
2. **`sim_benchmark.py`** – forms every enabled benchmark for every Wednesday from `inception_date` to the latest date FMP has published data for. The FMP screener has no history, so the universe stored by step 1 is used; the **historical** market cap of each of its companies' screened listings is fetched and converted to USD at that date's exchange rate. Per date, each company takes its primary listing's value (never a sum of listings) and the benchmarks are formed exactly as in step 6 – each benchmark's `market_cap_min` applied to that date's values. Report: `.output/sim_benchmark_report.md` – every snapshot.
3. **`sim_fund.py`** – for the chosen fund:
   - **erases** its existing `fund_holding`, `fund_holding_change` and `fund_analysis` rows (it refuses to run against production for this reason);
   - starting on the first Wednesday on or after `inception_date` (the live recalculation day, and the date of the `sim_benchmark.py` snapshots) and then every `recalc_frequency_days`, generates best ideas as of that date for all ETFs and recalculates the fund;
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
| `scripts/current_categorize_tickers.py` | The categorization ETF download, then the style assignment chain (step 4; [Style](#style-value--growth)). |
| `scripts/current_universe_screener.py` | Registers the FMP large-cap screen's home-market listings and stores the screen for the latest completed trading day (step 3, the Wednesday mode – follow with `current_ticker_values.py` to value it); `--register-only` only registers them (the other days). |
| `scripts/current_ticker_values.py` | The valuation pass of the ticker maintenance (step 4; [Prices and market caps](#prices-and-market-caps)): every ticker in use, and the lines of a screen stored for the day, valued for the latest completed trading day. |
| `scripts/data_fill_master_tickers.py` | The master-ticker sync of the ticker maintenance (step 4; [Share-class consolidation](#share-class-consolidation-master-tickers)) – see [Ticker data maintenance](#ticker-data-maintenance). |
| `scripts/current_universe_builder.py` | Builds the large-cap company universe from the latest stored screen (step 5) and lists the duplicates dropped and foreign lines admitted. Run the master sync first. |
| `scripts/current_benchmark_generator.py` | Forms every enabled benchmark from the latest stored universe (step 6). |
| `scripts/current_best_ideas.py` | Generates best ideas for all ETFs as of now (step 7) and prints the problems. |
| `scripts/current_model_fund.py` | Recalculates all funds as of today (steps 8–9). |

### Scraping and parsing

| Script | What it does |
|--------|--------------|
| `scripts/single_provider.py` | Scrapes one provider (`provider_id`), parses and resolves each ETF, prints the first/last rows, stores the holdings and values their tickers ([Prices and market caps](#prices-and-market-caps)). The quickest way to test a new or broken provider (use `--headed`). |
| `scripts/single_provider_etf.py` | Same for one ETF (`provider_etf_id`); also saves the downloaded file to `.output/downloads/`. |
| `scripts/download_and_save_all_providers.py` | Runs collection for every active provider, keeps every downloaded file in `.output/downloads/<provider>/`, and writes a report with holdings / resolved / problem ticker counts per ETF to `.output/downloads/report.md`. **Clears `.output/downloads/` first** (the rest of `.output/` is left alone). |
| `scripts/debug_etf_resolve_tickers.py` | Parses a file already in `.output/downloads/` (path relative to it, e.g. `<provider>/<file>`) for a given ETF and runs ticker resolution on it, without scraping. For tuning mappings and resolution. |
| `scripts/debug_holding_duplicates.py` | Read-only report of tickers appearing on several lines of one ETF/date, split into consistent (summed) and inconsistent (quarantined) groups. Report: `.output/holding_duplicates_report.md`. |

### Ticker data maintenance

| Script | What it does |
|--------|--------------|
| `scripts/data_fill_ticker_profile.py` | Refreshes every ticker profile not checked in a week ([Profile refresh](#profile-refresh)). Asks whether to retry invalid tickers too. |
| `scripts/data_fill_master_tickers.py` | Runs the master-ticker sync and the company market cap / region refresh ([Share-class consolidation](#share-class-consolidation-master-tickers)). Report listing every group (name-matched groups in detail, worth reviewing): `.output/master_tickers_report.md`. Run `data_fill_ticker_profile.py` first – CIKs come from the profiles. |
| `scripts/data_fill_categorization_esg.py` | Fills missing style and ESG for valid companies (not share-class siblings): the style chain (CAT_ETF → PROVIDER_ETF → MODEL) for tickers with no style, then ESG for companies never fetched. Asks whether to re-scrape the categorization ETFs first, retry tickers whose factor fetch failed (ignoring the 30-day back-off), and refresh ESG for all companies. Run after `data_fill_master_tickers.py`. Report with before/after coverage: `.output/data_fill_categorization_esg_report.md`. |
| `scripts/data_fill_ticker_value_gaps.py` | Finds gaps of more than 4 weekdays in `ticker_value` since 2026-01-01 for tickers held by ETFs, and fills them from FMP history (USD converted, glitches repaired – fetched with 60 days of context on each side). |
| `scripts/data_fill_ticker_value_refresh.py` | Rebuilds `ticker_value` history since 2026-01-01 from FMP's historical endpoints, **replacing** what's stored. Options: `--yes` (no confirmation), `--dry-run` (roll back), `--symbol AVGO` (one ticker), `--currency-mismatch` (only listings FMP quotes in a currency other than their exchange's – their history was stored converted from the exchange's currency before 2026-09), `--outliers` (only tickers whose stored market caps contain an FMP glitch – rewritten with the glitches repaired), `--profile-check` (only tickers with a stored market cap 2.5-fold off what their FMP profile's share count gives – rewritten against it; one profile call per ticker). Report: `.output/data_fill_ticker_value_refresh_report.md`. |

### Simulation and reporting

| Script | What it does |
|--------|--------------|
| `scripts/sim_prep_data.py` | Simulation step 1 – screen, ticker profiles, values, master sync and universe; see [Simulation](#simulation). |
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
| `screener_listing` | The weekly FMP large-cap screen: every line with its type, quote and (once registered) ticker |
| `universe_company` | The weekly large-cap company universe: one row per company with its region and USD market cap |
| `benchmark`, `benchmark_holding` | Benchmark definitions (region, cap, style, minimum market cap) and their weekly weights |
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
