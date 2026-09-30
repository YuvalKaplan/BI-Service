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
  - [3. Large-cap screener](#3-large-cap-screener)
  - [4. Ticker maintenance](#4-ticker-maintenance)
  - [5. Screened companies](#5-screened-companies)
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
  - [Index funds and size breakpoints](#index-funds-and-size-breakpoints)
  - [Free float](#free-float)
- [SEC active ETF list](#sec-active-etf-list)
  - [1. Finding the filings](#1-finding-the-filings)
  - [2. Reading a filing](#2-reading-a-filing)
  - [3. Updating the list](#3-updating-the-list)
  - [4. The current list](#4-the-current-list)
  - [5. Equity funds and their profile (FMP)](#5-equity-funds-and-their-profile-fmp)
- [Simulation](#simulation)
- [Backtesting](#backtesting)
- [Scripts](#scripts)
- [Environment variables](#environment-variables)
- [Hosting](#hosting)
- [Python](#python)
- [Database](#database)
- [Claude Code and the database MCP servers](#claude-code-and-the-database-mcp-servers)

---

## How the live pipeline works

```
SEC active ETF list ──► FMP profiles ──► selection rules ──► FMP ETF holdings ──► resolve to tickers ──► provider_etf_holding
                                                                        │
                                   FinancialModelingPrep (FMP) API ◄────┘  profiles, prices, market caps, ESG, factors
                                                                        │
Style reference ETFs ──► categorize_ticker ──► ticker.style_type ◄──────┤
                                                                        ▼
FMP screener ──► screener_listing ──────────────────────────────► ticker, ticker_value
                          │                                             │  ▲
                          │                                             ▼  │  master sync: listings → companies
                          ▼                                             │
                  screener_company ◄────────────────────────────────────┤
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
| Tue – Sat | Holdings collection from FMP (step 2), then the selection rules (step 1) → large-cap screener (step 3) → ticker maintenance (step 4): profiles → values → companies → style |
| Sun | Ticker maintenance, weekly part (step 4): style reference ETFs → ESG. Then the [SEC active ETF list](#sec-active-etf-list) takes in the week's new N-CEN filings and profiles its funds on FMP (its own process), and the selection rules (step 1) decide which ETFs are active – a new one is downloaded from Tuesday |
| Wed | *after the Tue–Sat steps, whose screener also stores the screen that day:* [index funds and size breakpoints](#index-funds-and-size-breakpoints) → [free floats and float factors](#free-float) → screened companies (step 5) → benchmark generation (step 6) → best ideas (step 7) → fund updates (steps 8–9) |
| Mon | Nothing |

Collection runs Tue–Sat: FMP refreshes most funds' holdings once a week (dated Sunday) and some daily or on another weekday, so every run picks up whatever changed. Every day, listings come in first – the holdings and the large-cap screener – and then the ticker maintenance goes over every ticker (profiles, values, companies, style – the [ticker utilities](#ticker-utilities)), so the Wednesday generators only use maintained tickers. Each step reads what the previous one stored, so any of them can be re-run on its own: on Wednesday the companies are built from the stored screen and the linked listings, and the benchmarks are formed on the same day the best ideas and funds consume them. If a step fails the cron stops and emails the admin: in particular the FMP screener is retried 3 times and then fails the run, and no companies or an empty benchmark (no market cap could be validated) fails it too, so best ideas and funds never run on a partial or missing benchmark.

When the run finishes (or fails), an email summary goes to the admins (step 10).

### 1. Providers and ETFs

`modules/sec/etf_profile.py` (Sundays) → `modules/sec/etf_selection.py` (after every profile run and holdings download)

The ETFs come from the [SEC active ETF list](#sec-active-etf-list), not from configuration: every actively managed US ETF whose own N-CEN filing and FMP profile show it to be an equity fund is a row of

- **`provider`** – the fund's company (FMP's `etfCompany`, e.g. JPMorgan, Capital Group);
- **`provider_etf`** – the ETF, keyed by its SEC series id, with [its profile](#5-equity-funds-and-their-profile-fmp): `region` (US / International / Global), `cap_type` (large / mid / small / smid / all), `style_type` (value / growth / blend, large-cap funds), stock holdings, sector weights and more – plus its `status` and `benchmark_id`.

**`fund`** is a model fund we build, with its `strategy` stored as JSON (step 8).

**Selection rules.** Which ETFs feed best ideas is decided by rules, not by hand. An ETF is **active** when it's still on the SEC list, its profile still finds it an equity fund, and:

| Rule | Passes when |
|------|-------------|
| Region | US or International (a Global fund has no benchmark and no fund to feed) |
| Cap size | `large` or `all` |
| Stock holdings | 20 to 200 (lines matching an index fund's stock) |
| Top sector | at most 40% of the fund (FMP's sector weights; when FMP has none, our tickers' sectors) |
| Holdings date | its latest holdings in `provider_etf_holding` at most 10 days old (FMP refreshes most funds weekly) |

A value we don't know fails its rule. A fund that moved to another trust is on the SEC list twice under one ticker until its old series drops off (BRIF and TGLR in September 2026) – only the one with the latest filing can be active; the other fails as a **duplicate** (it would download the same holdings and count twice in the funds). Otherwise the ETF is **inactive** – except one that passes everything but has no holdings downloaded yet, which stays **pending** (every new ETF's status) until its first download. The rules run after every Sunday profile run and every holdings download (step 2), so a new ETF on the SEC list that passes is downloaded from the next Tuesday. The selection also sets each ETF's `benchmark_id` – its region's large-cap blend benchmark (step 6). There's no reason column: the admin email counts the ETFs failing each rule and lists those that changed status, and [`scripts/current_sec_active_etfs.py`](#sec-data) writes each ETF's failed rules.

Every fund draws on all the active ETFs (a fund's `provider_etfs` can still limit it to some, step 8). The providers and ETFs that used to be configured and approved by hand, with holdings scraped from the managers' websites, are kept as `old_provider`, `old_provider_etf` and `old_provider_etf_holding`.

### 2. Holdings collection

`modules/cron/etf_downloader.py` – Tue–Sat, first

1. The active and pending ETFs are downloaded, and those failing only the holdings-date rule (so they come back as soon as FMP refreshes them): FMP's `etf/holdings` for each, 4 at a time.
2. The holdings are dated by FMP (the latest `updatedAt` of the fund's lines; a fund without one is skipped) and stored in `provider_etf_holding` under that date, replacing any stored for it – every line, with FMP's symbol, name, ISIN and CUSIP, shares, market value and weight.
3. Each line is resolved to a ticker, the first that applies:
   - one of our tickers with its FMP symbol, ISIN or CUSIP (no FMP call);
   - none for a line that isn't a stock by its name (cash, currencies, money-market funds, derivatives, bonds) or has no positive weight;
   - the ticker the same line (symbol, ISIN, CUSIP and name) had in the ETF's previous holdings – FMP lists some stocks by name only ("SAMSUNG ELECTRONICS CO"); a line left unresolved before is tried again on Wednesdays only, before the generators;
   - [Registration](#registration) from FMP.

   A line without a ticker is kept (with FMP's identifiers) but takes no part in best ideas. Prices and market caps are stored afterwards, once per ticker, by the ticker maintenance (step 4).
4. The selection rules (step 1) run again on the new holdings dates.

More than 10% of the ETFs failing fails the run. The admin email gives the ETFs stored, the lines by how they were resolved, the ETFs with the most unresolved stock weight, and the selection's summary.

### 3. Large-cap screener

`modules/cron/screener.py` – Tue–Sat, after holdings collection

The investable large caps come from the FMP company screener, from the lowest benchmark cutoff × 0.8 (the cutoffs are market-relative – [Index funds and size breakpoints](#index-funds-and-size-breakpoints); about $5.9B in September 2026: International's $7.4B × 0.8 – a company's whole cap is at least its float cap, so every company that can pass is screened). Every day its listings are registered in the `ticker` table alongside the ETF holdings' (step 2), so the ticker maintenance (step 4) covers everything the Wednesday generators use; on Wednesdays the screen is also stored for the screened companies (step 5).

1. The FMP company screener is paged through once per exchange: NYSE, NASDAQ, AMEX, TSX, LSE and 35 other international exchanges (Japan, Germany, Hong Kong, Australia, Switzerland, France, China, India, Taiwan, Korea, the Nordics, …). FMP compares `marketCapMoreThan` against each listing's **local-currency** market cap, so the USD threshold is first converted into the exchange's currency at the latest FX rate (an exchange with no known currency is skipped). The log shows each exchange's local threshold and smallest company returned, in USD.
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
- **Sunday**: the style reference ETFs' holdings are read from FMP (`modules/cron/categorize_downloader.py` – the reference data of [Style](#style-value--growth)), then every company's [ESG](#esg-qualification) data is refreshed.

So by the time the Wednesday steps run, every listing they can use has a current profile, a validated value for the day, its company and a style.

### 5. Screened companies

`modules/cron/company_builder.py` – Wednesdays, after the Tue–Sat steps

Turns the stored screen (the latest on or before today) into one row per company, using the companies as the master sync left them (step 4, [Share-class consolidation](#share-class-consolidation-master-tickers)):

1. **Home-market lines**, each at its latest validated market cap on the screen date or up to 5 days before – a market closed that day keeps its last close (all of JPX on a Japanese holiday). A listing without one (withheld all week) or flagged invalid is left out, and so is a note or preferred line by its current data – FMP reports no equity float for it (`ticker.free_float` = 0, see [Free float](#free-float)) or its FMP name is cut after a coupon ("TransCanada PipeLines Limited 6"). A company screened only through such a line – Algonquin through its notes `AQNB`, whose FMP cap is the note's price × Algonquin's shares (about $19B against a real $4B), Brookfield Renewable through `BEPI`, Santander UK through its preference shares – is left out.
2. **Foreign lines** are admitted only when they duplicate no company already in – not a listing of one, no shared ISIN, no matching company name of the same domicile – and are quoted in their exchange's currency. What's left is a company listed only outside its domicile (dsm-firmenich in Amsterdam, Prada in Hong Kong); those lines are registered as in step 3 and valued as in [Prices and market caps](#prices-and-market-caps).
3. **One row per company** – represented by its master, at its company market cap (`ticker.company_market_cap`, else the listing's own value), with its `ticker.region`.
4. **Duplicate guard** – drops any company the sync failed to merge (same evidence as [Share-class consolidation](#share-class-consolidation-master-tickers), plus a matching name for a company not anchored in its home market), keeping home-market companies first, across both regions.

The result is stored in `screener_company` for the screen date. No market-cap floor is applied here – each benchmark applies its own (step 6). Every skip and drop is logged and counted in the admin email; the sim prep report lists them.

### 6. Benchmark generation

`modules/cron/benchmark_generator.py` – Wednesdays, after the company builder

Forms every enabled benchmark of the `benchmark` table from the latest stored screened companies. Each row specifies which companies it holds:

| Benchmark | `region` | `cap_type` | `style_type` | `market_coverage` | Cutoff (Sep 2026) |
|-----------|----------|------------|--------------|-------------------|-------------------|
| US Large Cap Blend | `US` | `large` | `blend` | 0.93 – the Russell 1000's share of the market | ~$10.1B (~720 companies) |
| Intl Large Cap Blend | `International` | `large` | `blend` | 0.80 | ~$7.4B |

The large-cap line is **relative to the market**, not a fixed amount: a benchmark's cutoff is its market's breakpoint at its `market_coverage` – the float cap at which the market's largest companies make up that share of its total float cap, per the index funds' latest snapshot ([Index funds and size breakpoints](#index-funds-and-size-breakpoints)). A benchmark holds the screened companies of its `region` whose **whole company cap** reaches the cutoff with at least **10% floating** (`ticker.float_factor`; no factor known passes) – the S&P / Russell way: membership on the whole cap, a minimum float – and of its `style_type` (`blend`/`core`: any style; `value`/`growth`: the company's `ticker.style_type`). Each is weighted by the **whole company's** market cap (`weight = market cap / total`) – the size managers look at. The snapshot is stored in `benchmark_holding`, dated at the companies' screen date – Tuesday's close for the Wednesday run (a re-run replaces it); the cron email shows each benchmark's cutoff. An empty benchmark fails the run.

### 7. Best ideas

`modules/cron/best_ideas_generator.py` – Wednesdays

A manager's best ideas are the stocks they hold at a **higher weight than the market would**. For each active ETF:

1. **Holdings** – the latest holdings downloaded in the last 10 days (the selection's holdings-date limit), with duplicate lines summed or quarantined ([Registration](#registration)).
2. **Market caps** – for each holding, the latest market cap within 5 days of the holdings date. Holdings without one are reported as stale. If fewer than **95%** of the holdings have a market cap, the ETF is skipped for the week and reported.
3. **Company level** – siblings are folded into their master and the ETF's exposure across share classes is summed.
4. **Active weight** for each company:

   ```
   etf_weight       = company's market value in the ETF / ETF total market value
   benchmark_weight = company's weight in the reference benchmark
   delta            = etf_weight − benchmark_weight
   ```

   Two reference benchmarks are used, and both results are stored in `best_idea` with a `benchmark_mode`:

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
| `cap.name` | `large` (the company passes its region's full-universe benchmark cutoff – step 6), `mid_small` (it doesn't), or anything else for no filter |
| `style` | `value`, `growth`, or `blend`/`core` for no filter. A `blend` with `value`/`growth` percentages fills those shares of the fund from each style separately. |
| `region` | `US`, `International`, or a `split` with `US`/`International` percentages (exactly these keys — any other key is rejected); omitted = no filter |
| `provider_etfs` | limit to these ETFs' ideas (empty = all ETFs) |
| `exchanges` | limit to stocks on these exchanges |
| `esg_only` | only ESG-qualified companies |
| `ranking_from` / `ranking_to` | which ranks of each ETF's best ideas count (e.g. 1–3 = each manager's top three) |
| `recalc_frequency_days` | the fund is skipped until this many days have passed since its last recalculation |

**Building the candidate list.** All stored best ideas (latest per ETF) are filtered by the strategy:

- **Region** – `US` takes ideas from US ETFs in companies whose `ticker.region` is `US`; `International` takes ideas from international ETFs in companies whose region is `International` (region = primary listing, see [Share-class consolidation](#share-class-consolidation-master-tickers)). With a split, the international list is built first and those companies are excluded from the US list.
- **Cap** – large is the benchmark's rule (step 6): the company's cap on the date against its region's cutoff for that date, with at least 10% floating. For a `large` fund, a stock that has fallen below the cutoff is still allowed *only if the fund already holds it*, so it isn't sold for that reason alone; it can never be a new buy.
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
7. [Index funds and size breakpoints](#index-funds-and-size-breakpoints) – weekly (Wednesday, before the free float): the index funds' holdings and where the market's size lines fall.
8. [Free float](#free-float) – weekly (Wednesday, before the company builder), per listing and per company.

| Utility | Module | Used by |
|---------|--------|---------|
| Registration | `resolver.py` (holdings), `modules/cron/screener.py::register_listings` (screen) | steps 2, 3 and 5 (foreign lines) |
| Profile refresh | `refresh.py` | step 4, `sim_prep_data.py`, `data_fill_ticker_profile.py` |
| Prices and market caps | `valuation.py` (which tickers, once each), `pricing.py` (validation, glitches, USD) | step 4, step 5 (foreign lines), `sim_prep_data.py`, `current_ticker_values.py` |
| Share-class consolidation | `master.py`, `company.py` (primary listing), `identity.py` (same-company evidence) | step 4, `sim_prep_data.py`, `data_fill_master_tickers.py` |
| Style | `style.py`, `modules/calc/classification.py` | step 4, `current_categorize_tickers.py` |
| ESG | `esg.py`, `modules/calc/esg.py` | step 4 (Sunday), registration of a new ticker |
| Index funds and size breakpoints | `index_funds.py` | Wednesday before the free float (`current_index_funds.py`); read by the free float, the screener (step 3), benchmarks (step 6), the funds' cap filter (step 8), the sim, the SEC ETF profiles |
| Free float | `free_float.py` | Wednesday before step 5, `sim_prep_data.py`, `current_float_factors.py` |

### Registration

`modules/ticker/resolver.py` (holding lines), `modules/cron/screener.py::register_listings` (screener lines)

Each holding line FMP lists (step 2, and the style reference ETFs' – [Style](#style-value--growth)) is matched to a company using the FinancialModelingPrep (FMP) API (`TickerResolver.resolve_fmp_line`):

- by its **symbol**, which is already FMP's – when that symbol's FMP profile is the line's security: the ISIN (else the CUSIP) agrees, or, with neither to compare, the names match (a fund's symbol can be another company's: FAB is First Abu Dhabi Bank in a fund's holdings and a First Trust fund on FMP);
- else by its **ISIN** (FMP's ISIN search, then the profile);
- else by its **name** (FMP name search), accepting a candidate only with a verified name match.

  A name match requires every meaningful word of the shorter name to match the longer one and cover more than half of it (legal suffixes such as *Inc*, *PLC*, *Holdings* are ignored). A single shared word is never enough – loose matching would otherwise map unrelated holdings onto the same ticker.

When a profile is found, the ticker is inserted or updated with its identifiers (ISIN, CUSIP, CIK), name, exchange, sector, industry, country and currency. It's **marked invalid** (and ignored from then on) when it is crypto, has no company name or market cap, or its name identifies it as a fund, ETF, trust or index. New tickers get their ESG data straight away ([ESG](#esg-qualification)).

Results are cached for the duration of a run, so a stock held by many ETFs costs one lookup.

The screener registers its lines by exchange and symbol instead (`screener.register_listings`): a listing already known is used as it is, a new one is added with its FMP profile. Registration stores no value – see [Prices and market caps](#prices-and-market-caps).

**Duplicate lines.** A ticker can still appear on several lines of one ETF on one date. When best ideas are computed, those lines are summed if they all imply the same share price (within 2%) – they are lots of the same security. If the prices disagree, they are most likely different securities that resolved to the same ticker, so all of that ticker's lines are **quarantined** (excluded) and reported.

### Profile refresh

`modules/ticker/refresh.py::refresh_ticker_profiles`

Every day of the ticker maintenance (Tue–Sat), every ticker whose profile hasn't been checked for a week is refreshed from FMP (ISIN, CUSIP, CIK, name, sector, industry, country, currency, actively trading, and average daily turnover for the [primary listing](#share-class-consolidation-master-tickers) rule). A ticker is marked invalid if its profile can't be fetched, turns out to be crypto or a fund/ETF, or is no longer actively trading. A ticker that checks out again has its invalid flag cleared. When a ticker's currency changes (or becomes known and differs from its exchange's), its `ticker_value` history since 2026-01-01 is rewritten from FMP converted from the new currency – otherwise every new value would disagree with the stored ones and the ticker would end up marked invalid. Likewise, when any stored market cap is 2.5-fold off what the profile's share count gives at that day's price (a history on a wrong share count throughout has no jump for the glitch filter to catch), the history is rewritten against the profile's share count. A history off the profile's share count by less than that – 1.15- to 2.5-fold on each of its latest 10 values – is checked against a second source: FMP's quarterly financials (their weighted-average share count), else – for a company's own row – the [index funds](#free-float) (their float cap within 15% of the quote's cap × free float). When either agrees with the profile, it's FMP's history that carries the wrong count (Rocket Companies: 3.79B shares against the quote's and financials' 2.82B, putting it at $60B instead of $45B; Omnicom and Devon Energy after their mergers, where the financials still showed the old count and the index funds backed the quote), so the profile's count becomes the ticker's **verified share count** (`ticker.verified_shares`) and the history is rewritten to it. The profile isn't trusted on its own – when the financials and the index side with the history instead (SABESP), nothing changes. A verified count follows the profile while a second source still agrees and is cleared when neither does.

### Prices and market caps

`modules/ticker/valuation.py` (which tickers, once each, in parallel), `modules/ticker/pricing.py` (validation, glitches, USD)

Once a day, in the ticker maintenance, every ticker in use gets its price and market cap for the latest completed trading day stored in `ticker_value` (`pricing.latest_value_date`: before 17:00 New York time that is the previous day, and a weekend steps back to Friday – the date every step stores and reads values under): the valid tickers in each active ETF's latest holdings from the last 10 days (the holdings best ideas can use – the selection's holdings-date limit; about 3,600) and, on Wednesdays, the registered lines of the stored screen (about 900 more). Each ticker is valued once however many ETFs hold it, 5 in parallel (FMP's rate limit of 200 calls a minute is the bound), and one already valued for the date is skipped. The company builder values the few foreign lines it admits the same way.

FMP's profile endpoint occasionally returns wrong values, so the numbers are taken from FMP's **historical** price and market-cap endpoints and **validated**: the last 21 days fetched now are compared against what we stored before. If any overlapping day differs by more than 0.5% (price) or 1% (market cap), today's value is withheld. If a ticker has gone 5 days without a good value, it's marked invalid with the mismatch details.

FMP's market-cap **history itself has glitches** – a wrong unit or share count for a day or for months: Compass at 1/100 (pence vs pounds) for two weeks in June 2026, a NYSE line at ×266 every weekend, and a late-July share-count change that put Hyundai Glovis at ×7.7 and Prudential's Hong Kong line at ×10 while fixing Centrica's, SPIE's and Hanwha Ocean's older history. Every history fetch (validation, rebuilds, the sim) therefore cleans the market caps first (`pricing.clean_market_caps`), on a year of context: a market cap doesn't move 3-fold in a day unless its price does (a crash moves both – EyePoint's -67% day is kept), so such jumps split the series into runs. A spike or streak that comes back is a glitch; for a jump that stays, FMP's **current quote** of the listing (market cap ÷ price = its share count; from the screener row, else the profile) decides which side is wrong – values whose implied share count is 2.5-fold off it are the glitch. The quote itself is ignored when the only values it agrees with are a spike or streak that came back (SABESP's profile carries the same wrong share count as an eight-day glitch in August), or when it disagrees with an entire established history; against a new line it wins (Uniper's new XETRA line: every value on 8.7B shares, the quote 416M). A glitch is **repaired** as that day's price × the share count of the nearest good values (or the quote's), so the value stays fresh; when FMP scaled the price as well (≥ 20-fold, pence vs pounds) it is dropped. The profile is only fetched for a series that shows a glitch, a 2-fold move the price didn't make (DuPont's share count went 410M → 137M: exactly 3×), or is new; screened listings use the screener's quote at no cost. A listing with a **verified share count** (see [Profile refresh](#profile-refresh)) has every value of its history's latest run – walking back from the newest value, as long as the implied share count stays within 5% of the newest one – that is 1.15-fold or more off it set to that day's price × the verified count first. The verified count is today's, so an earlier run on another count is a real change and left alone (Devon Energy: 621M shares until its merger, 1,100M after – only the run since August, on FMP's lagging 937M, is rewritten) – on every fetch, so each day's new value matches the rewritten history.

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
- **Primary listing** – listings are ranked: active ones first (a market cap in the last 30 days, so an old ticker left behind by a ticker change drops out), ordinary shares before preferred, note, depositary, when-issued and unit lines (including Korean preferred codes, which don't end in 0, lines FMP reports with no equity float, and FMP names cut after a coupon or series number – "Southern Company (The) Series 2", "KKR Group Finance Co. IX LLC 4.") and before *thin* lines – a line whose average daily turnover (`ticker.average_turnover`, FMP's average volume × price) is under 5% of the company's busiest ordinary line on the same country's exchanges, in the same currency (NYSE, NASDAQ and AMEX together; NSE with BSE; XETRA with Frankfurt; OTC only with OTC). FMP names some units, notes and preferreds exactly like the company and stamps its whole market cap on them – The Southern Company's 2025 corporate units `SOMN`, ANZ's capital notes `AN3PJ` (whose cap FMP computes at the note's price, 2.7× ANZ's), Comcast's exchangeable debentures `CCZ` on NYSE (against its shares, `CMCSA`, on NASDAQ) – so only their turnover gives them away (they trade 0.1–3.5% of the ordinary shares); a thinly traded share class (Carlsberg A, McCormick's voting shares) or venue (BSE against NSE) ranks behind the main one too. Then by market – the domicile country's exchanges (HK counts as home for Chinese companies), then for Dutch and Luxembourg holding companies the other continental exchanges they typically list on (Airbus and Euronext in Paris, Stellantis and Tenaris in Milan, argenx in Brussels), then a US listing (for US-listed, foreign-domiciled companies such as Eaton or Medtronic), then other countries' exchanges, then OTC and LSE's International Order Book – then lines quoted in their exchange's own currency before others (Tencent's HKD line before its RMB counter), then the master. The first is the primary listing.
- `ticker.company_market_cap` (master only) is the primary listing's latest market cap. FMP reports the **whole company's** market cap on every listing, so listings are never summed. It counts every share class, listed or not, and an Up-C company's LLC units (Carvana: Class A plus the unlisted Class B, ~1.1B shares, where sources quoting the listed class alone count ~0.72B and show a cap about a third lower; Alphabet's unlisted Class B likewise). Best ideas and funds measure a company **as of the holdings date** instead: the first listing in the same ranking with a value near that date, so historical (sim) dates use that date's values.
- `ticker.region` (every listing) is the company's region: `US` when the primary listing trades on NYSE/NASDAQ/AMEX (or, for a US company, on OTC), else `International`. TSM, ASML, SAP, Shopify, STMicroelectronics and ArcelorMittal are International; Eaton, Medtronic, Linde and ARM (an ADR with no UK listing) are US.

Benchmarks, best ideas and funds all work at company level: a sibling is replaced by its master, the company market cap is used instead of a single listing's, and US/International is decided by `ticker.region`.

### Style (value / growth)

Funds can be restricted to value or growth stocks, so each company needs a `style_type`.

**Reference data (Sundays)** – `modules/cron/categorize_downloader.py`

A list of index-style ETFs with a known style and cap size is kept in `categorize_etf` by ticker: Vanguard's VUG / VTV (large growth / value) and VBK / VBR (small), and iShares' Morningstar ILCG / ILCV, IMCG / IMCV and ISCG / ISCV (large, mid and small growth / value). Their holdings are read from FMP (`etf/holdings`), and every stock constituent is resolved like a provider ETF's ([Registration](#registration)) into `categorize_ticker` with that ETF's style and cap type, plus a set of fundamental factors from FMP (growth rates, margins, valuation ratios, yields).

**Assigning style to tickers (Tue–Sat, after the master sync)** – `modules/ticker/style.py`, in order of preference, only for tickers with no style yet (the `type_from` column records the source):

1. `CAT_ETF` – the ticker is a constituent of a categorization ETF (same symbol and exchange): take its style and cap type.
2. `PROVIDER_ETF` – the ticker is held by a provider ETF whose profile is value or growth (60% of its style-classified weight): take that style.
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

### Index funds and size breakpoints

`modules/ticker/index_funds.py::refresh` – weekly, the first of the Wednesday steps (before the free float); `scripts/current_index_funds.py` runs it by hand and prints the breakpoints and the benchmarks' cutoffs.

Size is measured against the market itself, as the index providers do, rather than at fixed dollar lines (a $2B / $10B line calls today's small-cap funds mid and mid-cap funds large). The market is what three Vanguard index funds hold, each company at its float-adjusted weight – the funds are the rows of `universe_etf`:

| Fund | Index | Market |
|------|-------|--------|
| VTI | CRSP US Total Market | US |
| VEA | FTSE Developed All Cap ex US | International |
| VWO | FTSE Emerging Markets All Cap China A Inclusion | International |

1. Each fund's holdings (FMP) are stored as a weekly snapshot in `universe_etf_holding` – kept, so a cutoff, a float factor or a fund profile can be traced back to the holdings behind it (about 12,500 lines a week; a re-run the same day replaces it). A line gets the `ticker_id` of the ticker it matches, where we have one (none are registered for it).
2. A fund's lines are grouped into **companies** – by our company (all its share classes and listings), else by the issuer part of the CUSIP – and valued as **float caps**: the company's value in the fund × the fund's scale (float cap per $ held – the median over our companies of free float × company cap ÷ value; VTI ~31, VEA ~102, VWO ~75). FTSE holds China A shares (VWO's `.SS` / `.SZ` lines – 41% of its lines, 6% of its weight) at 25% of their float, so they are scaled up by it first – left as held they pulled VWO's scale 20% up and put those companies at a quarter of their size. Each line stores its company's float cap.
3. **Breakpoints** (`market_breakpoint`) – for each market (US: VTI; International: VEA + VWO, a company both markets hold counting as US) and each coverage from 50% to 99%: the float cap at which the market's largest companies make up that share of its total float cap. In September 2026:

| Coverage | US | International |
|----------|----|---------------|
| 70% | $94.3B (122 companies) | $14.6B (551) |
| 80% | $49.6B (226) | **$7.4B** (983) – the International benchmark |
| 90% | $16.3B (473) | $3.1B (1,908) |
| 93% | **$10.1B** (637) – the US benchmark, the Russell 1000's share | $2.2B (2,413) |

The breakpoints are read by date (`index_funds.cutoff`): the benchmarks' cutoffs (step 6, each at its `market_coverage`), the funds' large / mid_small filter (step 8, its region's full-universe benchmark's), the screener's threshold (step 3), the free float's cap check, the simulation's historical benchmarks (the snapshot on or before each Wednesday – the earliest one for dates before snapshots were kept), and the SEC ETF profiles' size classes (70% / 90%). A stored snapshot more than a week old is refreshed before it's read; if the index funds can't be downloaded the Wednesday run stops before the free float.

### Free float

`modules/ticker/free_float.py::refresh` – weekly, Wednesday, after the index funds and before the company builder; `scripts/current_float_factors.py` runs it alone and writes a report (`.output/float_factors_report.md`).

Benchmarks weigh each company by its **whole** market cap – the company's size, which is what managers look at. Free float and the holdings of the Vanguard index funds (their stored snapshot – [Index funds and size breakpoints](#index-funds-and-size-breakpoints)) are kept as **safeguards** around it:

- **`ticker.free_float`** (every listing) – FMP's free float % (`shares-float-all`). FMP reports **0** for an exchange-traded note or preferred named like its issuer (`AQNB`, `BEPI`, `CCZ`): such a line counts as non-equity – it ranks behind the ordinary shares and is left out of the screened companies (step 5), so a company screened only through its notes (Algonquin: FMP puts it at $19B, the note's price × Algonquin's shares, against a real $4B) isn't in the benchmark. A 0 on a listing an index fund holds, or on one sharing its ISIN with a listing that is held or has a float (the same shares on another venue), is a data gap and kept empty instead.
- **`ticker.float_factor`** (every company) – the investable share of its market cap – normally 0–1, above 1 when the index holds more than our cap (see the check below) – from the holdings of **VTI**, **VEA** and **VWO** (China A shares scaled up by their 25% inclusion), which hold nearly every listed company at its float-adjusted weight – a company more than one holds is measured by the fund of its region: a company's market value in the fund, summed over its share classes (GOOGL + GOOG), scaled to its float cap, over its company market cap (the scale per fund is set so the factors sit on FMP's free-float scale). A company no index fund holds – MLPs, BDCs, US-sanctioned Chinese companies, companies below the index's minimum float (Christian Dior) – takes its primary listing's FMP free float. No weight uses it (Microsoft 0.94, Tencent 0.70, Carvana 0.65, Interactive Brokers 0.25, SpaceX 0.04 in September 2026), but benchmark membership and the funds' large-cap filter require at least 0.10 (S&P's minimum float).
- **Market-cap check** – an index fund can't hold more of a company than the whole company. For every company at or above its region's large-cap cutoff that an index fund holds, the fund's shares are valued at our own price on the date of our company cap (the funds' reported market values are often out of line with their share counts; valued this way fully floated companies come out at 0.97–1.01 of their cap), and a float cap 1.15× the company market cap or more means the company cap is on too few shares (as Bitmine's history was: 230M shares against 570M). The company is listed in the report and counted in the cron email. In September 2026 that flagged five: Omnicom and Devon Energy (histories still on their pre-merger share counts – FMP's quote and the index agree on the new ones), Equity Residential (a renamed ticker we still carry the old line of), and Standard Life and Delta Electronics (our caps agree with FMP's profile and financials – most likely the index holding the company under two lines). A flagged company's share count is then checked on the spot, with the index funds as a second source for the quote ([verified share count](#profile-refresh)): when the quote's cap agrees with the index, its count becomes the verified share count, the history's latest run is rewritten and the company caps are refreshed – before the company builder runs. That fixed Omnicom (history on 205M shares, quote and index 274M: $15.6B → $20.6B) and Devon Energy (937M → 1,100M: $44.1B → $51.4B) in September 2026. Standard Life, Delta Electronics and Equity Residential stayed flagged for review – their quotes agree with our history, or there's no current quote. (A cap on too many shares – Rocket Companies' history on 3.79B against 2.82B – is caught by the verified share count's own check.)

If the index funds' snapshot or the free floats can't be read, last week's values stay (logged).

## SEC active ETF list

A list of every actively managed US ETF, built from the funds' own filings with the SEC – the source of the ETFs best ideas are drawn from ([Providers and ETFs](#1-providers-and-etfs)). It's a process of its own (`modules/sec/`), apart from the live pipeline's steps. The daily cron runs it every Sunday ([Weekly schedule](#weekly-schedule)); [`scripts/current_sec_active_etfs.py`](#sec-data) runs it by hand.

Every registered fund files **Form N-CEN** with the SEC once a year, within 75 days of its fiscal year end, and states its own type in it (Item C.3): exchange-traded fund, index fund, fund of funds, a multiple or inverse of a benchmark, and so on. **An ETF that isn't an index fund is actively managed.** The flag is the fund's own statement and holds up: GSLC (ActiveBeta), JQUA, the BetaBuilders funds, BOUT/FFTY and NIXT are index funds by their filings, and they do track an index.

### 1. Finding the filings

`modules/sec/edgar.py` reads EDGAR directly. Its quarterly form index lists every N-CEN / N-CEN/A filing – about 450 a quarter, about 1,900 in the first quarter (the December fiscal year ends). Each run reads the last five quarters' indexes (a full filing year, plus slack for late filers) and keeps the filings not read yet (`sec_ncen_filing`): the first run reads about 3,900 filings (15–25 minutes), a weekly run only the week's new ones. Downloads run four at a time, together at most 5 requests a second (half the SEC's limit), each carrying the `SECRET_SEC_USER_AGENT` the SEC requires.

Not the SEC's quarterly N-CEN data sets: they appear weeks after the quarter, and the 2025 Q4 set lacks 97 of the quarter's 598 filings (Amplify ETF Trust and Morgan Stanley ETF Trust among them). EDGAR has every filing the day after it's made.

### 2. Reading a filing

`modules/sec/ncen.py::parse_filing` reads the filing's XML (`primary_doc.xml`): the registrant (trust), the report period, and per fund its series id, name, types, listed ticker (from the filing's exchange-listing section, else the share class), investment adviser and average net assets – plus the series terminated during the period and whether it's the registrant's last filing. A filing can cover only some of a trust's funds (trusts with several fiscal year ends file several). A filing that fails to download or parse is stored with its error (its XML saved to `.output/downloads/ncen/`) and read again on the next run; more than 10% of a run's filings failing fails the run.

### 3. Updating the list

`sec_active_etf` holds one row per fund (SEC series id), from its latest filing. Each filing, applied in one transaction:

- upserts its active ETFs (exchange-traded, not index funds);
- removes the funds it reports as index funds or no longer ETFs;
- marks the funds it lists as terminated – every fund it reports on when it's the registrant's last filing.

A row only ever moves to a newer filing (report period, then filing date), so filings can be read in any order, or again (`--reload`), with the same result.

### 4. The current list

Funds not terminated and with a filing in the last 15 months (a fund files every 12; one liquidated without a termination on record drops off). Funds launched since their trust's last fiscal year end aren't on it until their first N-CEN. Leveraged and inverse funds (`is_multiple_inverse`), funds of funds and exchange-traded managed funds are flagged, not left out; the filings carry no asset class or region – step 5 finds out what kind of fund each is.

### 5. Equity funds and their profile (FMP)

`modules/sec/etf_profile.py::run` – Sunday, after step 4. Every listed fund is checked when it's new, when a newer filing arrives, and every 28 days – an active ETF every Sunday, so the selection rules read a profile at most a week old (`--refresh-all` checks them all again): two FMP calls – `etf/info` (asset class, provider, description, website, AUM, NAV and its currency, inception date, expense ratio, sector weights) and its holdings. The first run checks ~2,500 funds (~25 minutes); a weekly run about 600.

**Its strategy** (`sec_etf_classification`, every checked fund, so a fund left out isn't checked again before its next refresh) – the first that applies:

1. `leveraged_inverse` – its filing says it seeks a multiple or inverse of an index, FMP's asset class says so, or its name (2x, Bull / Bear, Inverse…);
2. `fund_of_funds` – its filing says so (left out);
3. `buffer` – buffer, defined outcome, floor, structured protection… in its name (FMP files many under equity);
4. `option_income` – covered call, premium / option income, buy-write, 0DTE, high income… in its name;
5. `fixed_income`, `multi_asset`, `alternative`, `commodity`, `currency` – FMP's asset class;
6. `equity` – an equity asset class (or none) and at least **80% of its holdings in stocks**: a line matching an index fund's stock, or any line whose name isn't cash, a currency, a money-market fund, a derivative or a bond (FMP lists some emerging-market stocks by name only);
7. otherwise `equity_mixed` / `other`.

**Only `equity` funds go in the provider tables** – `provider` (the fund's company, from FMP) and `provider_etf` (a new one *pending* – [Providers and ETFs](#1-providers-and-etfs)). Each fund's profile, against the index funds' latest snapshot ([Index funds and size breakpoints](#index-funds-and-size-breakpoints)):

- **Region** (`region`) – the share of its matched stocks the US fund holds: US at 80%+, International at 20% or less, otherwise Global;
- **Cap size** (`cap_type`) – each stock large at or above its market's 70% breakpoint, small below the 90% one, mid between (Morningstar-style); the fund large with 70%+ of its stocks large, small with 50%+ small, mid with 50%+ mid, smid with 70%+ mid and small, otherwise all;
- **Value / growth** (`style_type`, large-cap funds only) – our companies' `style_type`, 60% for value or growth, else blend;
- **Stock holdings** (`stock_holdings`) – its lines matching an index fund's stock;
- **Sectors** (`sector_weights`, `top_sector`, `top_sector_weight`) – FMP's sector weights and the largest sector that isn't cash; when FMP has none (about 1 fund in 5), our tickers' sectors over the lines matching one of our companies, as shares of those lines, when they make up at least half the fund's stocks;
- AUM, NAV, expense ratio, inception (`trading_since`), website, the shares behind each measure, and its average float cap.

Region and size need half the fund matched to the index funds' stocks. The profile stores no holdings – step 2 downloads them. A fund that stops qualifying keeps its row with its new strategy, and the selection makes it inactive, as it does one that leaves the SEC list; then the selection rules run for every ETF (step 1). Samples (September 2026): CGDV, JGRO and DIVO large US; DFAC all; KMID and CGMM mid; AVUV, JSML, SMLL, BSVO and TMSL small; CGXU, NBJP and JADE International; JEPI / QQQI option income, BUFR a fund of funds, PJAN buffer, NVDL leveraged, JAAA fixed income – left out.

The script writes `.output/sec_active_etfs.csv`: every current fund with its strategy and, for the equity funds, their profile, status, latest holdings date and the rules they fail – next to the old hand-picked `old_provider_etf`'s cap / style / region where it had the fund – followed by the old enabled ETFs and what the rules make of them.

## Simulation

The simulation replays the Wednesday logic (best ideas → fund) week by week over the holdings already in the database, to see how a fund would have been built and changed since a chosen start date. It uses the exact same best-ideas and fund code as live.

Run against development, in this order:

```bash
python scripts/sim_prep_data.py --dev    # 1. screen, refresh profiles, value, sync master tickers, build the companies
python scripts/sim_benchmark.py --dev    # 2. backfill weekly historical benchmarks (edit inception_date first)
python scripts/sim_fund.py --dev         # 3. run the fund simulation (edit fund_id / inception_date / weeks first)
```

1. **`sim_prep_data.py`** – the same steps as live, in the same order: large-cap screener (step 3, storing the screen), the ticker maintenance's profile refresh, values and master-ticker sync (step 4), and the screened companies (step 5). The screen is dated at the latest date FMP has published data for (a few days behind today) rather than the latest trading day, and a listing whose value can't be validated on that date stays in the screened companies (the backfill fetches its whole history anyway). It asks whether to also retry tickers previously marked invalid. It's safe to re-run; it only picks up what's stale or new. Report: `.output/sim_prep_data_report.md` – including the duplicate companies dropped and the foreign / non-equity lines skipped or admitted.
2. **`sim_benchmark.py`** – forms every enabled benchmark for every Wednesday from `inception_date` to the latest date FMP has published data for. The FMP screener has no history, so the companies stored by step 1 are used; each of its companies' screened listings needs a **historical** market cap near every Wednesday. The stored `ticker_value` history is used when it has one for every Wednesday (about 96% of listings – ETF holdings are valued daily); only the rest is fetched from FMP (converted to USD at each date's exchange rate, glitches repaired as in step 4). Per date, each company takes its primary listing's value (never a sum of listings) and the benchmarks are formed exactly as in step 6 – each benchmark's cutoff for that date (its market's breakpoint in the index funds' snapshot on or before it – the earliest one for dates before snapshots were kept) applied to that date's values, with today's float factors. Report: `.output/sim_benchmark_report.md` – every snapshot.
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
- Several scripts have their parameters (`fund_id`, dates…) as variables at the top of the file, marked `# <-- edit before each run`.
- All script output goes to `.output/` (git-ignored): reports and simulation results at the top level or in their own subfolder (`methodology/`), and downloaded files kept for debugging in `.output/downloads/`.

### Live pipeline stages (run one stage by hand)

| Script | What it does |
|--------|--------------|
| `service_cron.py` | The full daily cron. Runs whatever is scheduled for today's weekday. |
| `scripts/current_etf_holdings.py` | The holdings download from FMP (step 2), then the selection rules (step 1). `--retry-unresolved` tries again the lines left unresolved before, as Wednesdays do – use it after the first run on a new database. |
| `scripts/current_categorize_tickers.py` | The style reference ETFs' FMP holdings, then the style assignment chain (step 4; [Style](#style-value--growth)). |
| `scripts/current_screener.py` | Registers the FMP large-cap screen's home-market listings and stores the screen for the latest completed trading day (step 3, the Wednesday mode – follow with `current_ticker_values.py` to value it); `--register-only` only registers them (the other days). |
| `scripts/current_ticker_values.py` | The valuation pass of the ticker maintenance (step 4; [Prices and market caps](#prices-and-market-caps)): every ticker in use, and the lines of a screen stored for the day, valued for the latest completed trading day. |
| `scripts/data_fill_master_tickers.py` | The master-ticker sync of the ticker maintenance (step 4; [Share-class consolidation](#share-class-consolidation-master-tickers)) – see [Ticker data maintenance](#ticker-data-maintenance). |
| `scripts/current_index_funds.py` | Downloads the index funds' holdings (VTI, VEA, VWO), stores the snapshot and the market's size breakpoints ([Index funds and size breakpoints](#index-funds-and-size-breakpoints)), as the Wednesday cron does before the free float, and prints the breakpoints and each benchmark's cutoff. |
| `scripts/current_float_factors.py` | Refreshes every listing's free float and every company's float factor ([Free float](#free-float)), as the Wednesday cron does before the company builder, and reports the large-cap market caps below the index funds' float cap (too low – worth checking), the screened companies without a factor, the lowest factors, and the listings found with no equity float. Report: `.output/float_factors_report.md`. |
| `scripts/current_company_builder.py` | Builds the screened companies (one row per company) from the latest stored screen (step 5) and lists the duplicates dropped and foreign lines admitted. Run the master sync first. |
| `scripts/current_benchmark_generator.py` | Forms every enabled benchmark from the latest stored screened companies (step 6). |
| `scripts/current_best_ideas.py` | Generates best ideas for all ETFs as of now (step 7) and prints the problems. |
| `scripts/current_model_fund.py` | Recalculates all funds as of today (steps 8–9). |

### Holdings

| Script | What it does |
|--------|--------------|
| `scripts/debug_holding_duplicates.py` | Read-only report of tickers appearing on several lines of one ETF/date, split into consistent (summed) and inconsistent (quarantined) groups. Report: `.output/holding_duplicates_report.md`. |

### Ticker data maintenance

| Script | What it does |
|--------|--------------|
| `scripts/data_fill_ticker_profile.py` | Refreshes every ticker profile not checked in a week ([Profile refresh](#profile-refresh)). Asks whether to retry invalid tickers too. |
| `scripts/data_fill_ticker_turnover.py` | Stores `ticker.average_turnover` now (instead of within a week, from the profile refresh) for every valid listing that shares its company and exchange with another, and lists the thin lines found. Run `data_fill_master_tickers.py` afterwards to move masters off thin lines. |
| `scripts/data_fill_master_tickers.py` | Runs the master-ticker sync and the company market cap / region refresh ([Share-class consolidation](#share-class-consolidation-master-tickers)). Report listing every group (name-matched groups in detail, worth reviewing): `.output/master_tickers_report.md`. Run `data_fill_ticker_profile.py` first – CIKs come from the profiles. |
| `scripts/data_fill_categorization_esg.py` | Fills missing style and ESG for valid companies (not share-class siblings): the style chain (CAT_ETF → PROVIDER_ETF → MODEL) for tickers with no style, then ESG for companies never fetched. Asks whether to re-read the style reference ETFs from FMP first, retry tickers whose factor fetch failed (ignoring the 30-day back-off), and refresh ESG for all companies. Run after `data_fill_master_tickers.py`. Report with before/after coverage: `.output/data_fill_categorization_esg_report.md`. |
| `scripts/data_fill_ticker_value_gaps.py` | Finds gaps of more than 4 weekdays in `ticker_value` since 2026-01-01 for tickers held by ETFs, and fills them from FMP history (USD converted, glitches repaired – fetched with 60 days of context on each side). |
| `scripts/data_fill_ticker_value_refresh.py` | Rebuilds `ticker_value` history since 2026-01-01 from FMP's historical endpoints, **replacing** what's stored. Options: `--yes` (no confirmation), `--dry-run` (roll back), `--symbol AVGO` (one ticker), `--currency-mismatch` (only listings FMP quotes in a currency other than their exchange's – their history was stored converted from the exchange's currency before 2026-09), `--outliers` (only tickers whose stored market caps contain an FMP glitch – rewritten with the glitches repaired), `--profile-check` (only tickers with a stored market cap 2.5-fold off what their FMP profile's share count gives – rewritten against it; one profile call per ticker), `--shares-check` (only tickers whose [verified share count](#profile-refresh) is set, changed or cleared now – rewritten against it; one profile call per ticker with recent history, one more for those flagged). Every rewrite applies the ticker's verified share count, as the live valuation does. Report: `.output/data_fill_ticker_value_refresh_report.md`. |
| `scripts/data_fix_symbol_encoding.py` | One-off repair (September 2026) of the tickers whose FMP symbol holds an `&` (NSE's `M&M`, `M&MFIN`, `J&KBANK`, `ARE&M`, `GVT&D`, MEX's `PE&OLES`). Before the FMP client encoded its query values, FMP answered for the part before the `&` – `M&M.NS` carried Macy's profile, identifiers and values and was grouped with Macy's (making Macy's International), `J&KBANK` Jacobs', `ARE&M` Alexandria's. Clears their master links and derived data, stores their own profile, rewrites their value history and runs the master sync. `--dry-run` lists them and what FMP returns now; `--yes` skips the confirmation. Run after the encoding fix is deployed. Report: `.output/data_fix_symbol_encoding_report.md`. |

### Simulation and reporting

| Script | What it does |
|--------|--------------|
| `scripts/sim_prep_data.py` | Simulation step 1 – screen, ticker profiles, values, master sync and company builder; see [Simulation](#simulation). |
| `scripts/sim_benchmark.py` | Simulation step 2 – historical weekly benchmarks. |
| `scripts/sim_fund.py` | Simulation step 3 – replays one fund. Development only. |
| `scripts/report_fund_methodology.py` | Read-only. For a `fund_id` and date (default: the fund's inception), writes to `.output/methodology/<fund>_<id>_<date>/`: `README.md` (strategy, constants, methodology), `etfs/<etf>_<id>.xlsx` (each ETF's holdings and active-weight calculation), `benchmark.xlsx`, `best_ideas.xlsx` (every idea and which fund filter it passed or failed) and `fund.xlsx` (holdings with their justification, and the candidates not selected). |

### SEC data

| Script | What it does |
|--------|--------------|
| `scripts/current_sec_active_etfs.py` | Updates the [SEC active ETF list](#sec-active-etf-list) as the Sunday cron does – the N-CEN filings on EDGAR not read yet, then the FMP profiles due (equity funds to the provider tables), then the selection rules ([Providers and ETFs](#1-providers-and-etfs)) – and writes `.output/sec_active_etfs.csv`: every current fund's strategy and, for the equity funds, their profile, status, latest holdings date and failed rules, next to the old hand-picked `old_provider_etf`'s cap / style / region where it had the fund, followed by the old enabled ETFs and their new status. `--reload` reads every filing in the window again; `--refresh-all` profiles every fund again. Needs `SECRET_SEC_USER_AGENT`. |

### Database

| Script | What it does |
|--------|--------------|
| `scripts/db_sync_dev_from_prod.py` | Copies the new `provider_etf_holding` rows from production to development (per ETF, everything newer than development's latest date), matching the ETFs by their SEC series id (each database builds its own list; an ETF development doesn't have yet is skipped with a warning) and remapping tickers by symbol + exchange, adding missing tickers. Production is opened read-only. Derived data is then regenerated by running the pipeline against development. Options: `--yes`, `--dry-run`, `--provider-etf-id N`. |

---

## Environment variables

Set in a `.env` file at the project root (loaded with `python-dotenv`; it's git-ignored – ask an admin for the values). VS Code's launch configurations load the same file.

| Variable | Used for |
|----------|----------|
| `ENV_TYPE` | `development` or `production`. Chooses the database when no `--dev`/`--prod` flag is passed. The hosted cron passes `--prod` instead ([Hosting](#hosting)). |
| `PYTHONPATH` | The project root, so `modules.*` imports resolve when running scripts. |
| `SECRET_DATABASE_HOST` / `_PORT` / `_USER` / `_PASSWORD` | Development PostgreSQL server (also used by backtesting). |
| `SECRET_DATABASE_NAME` | Development live database (`best_ideas`). |
| `SECRET_DATABASE_NAME_BT` | Backtesting database (`best_ideas_bt`), always on the development server. |
| `SECRET_DATABASE_PROD_HOST` / `_PORT` / `_USER` / `_PASSWORD` / `_NAME` | Production PostgreSQL, used with `--prod` or `ENV_TYPE=production`. |
| `SECRET_MARKET_DATA_API_KEY` | FinancialModelingPrep API key (profiles, prices, market caps, screener, ESG, factors, FX). Calls are limited to 200 per minute by the client. |
| `SECRET_MAILGUN_ENDPOINT` / `SECRET_MAILGUN_API_KEY` | Mailgun, for the admin emails. |
| `SECRET_HOLDINGS_DATA_API_KEY` | FactSet – only for the one-off backtesting historical holdings download. |
| `SECRET_SEC_USER_AGENT` | The User-Agent sent to EDGAR (the [SEC active ETF list](#sec-active-etf-list)): a name and a contact email, e.g. `BI-Service admin@example.com`. The SEC refuses requests without one. |
| `SECRET_TOKEN_AUTH_SIGN` | Signing secret for `modules/core/token.py` (URL tokens). Not needed by the pipelines. |

---

## Hosting

The live cron runs on the hosting service's standard Python runtime (Python 3.13):

- the build installs the dependencies: `pip install -r requirements.txt`;
- the hosting service's cron job runs `python service_cron.py --prod` once a day – `--prod` points it at the production database (setting `ENV_TYPE=production` does the same) – and defines the `SECRET_*` environment variables.

Everything the pipeline reads comes from APIs (FMP, EDGAR), so it needs no browser. It used to run from a Docker image, only because scraping the managers' websites needed a headless Chromium and the operating-system libraries behind it.

---

## Python

### Virtual environment

Create a virtual environment at the command prompt with:
`python -m venv .venv`

Activate it with:
`.venv/Scripts/activate` (Windows) or `source .venv/bin/activate` (Linux/macOS)

Install the dependencies:
`pip install -r requirements.txt`

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
`python-dotenv psycopg psycopg-pool psycopg-binary pydantic pandas openpyxl mailgun scikit-learn httpx bcrypt`

| Module | Used for |
|--------|----------|
| `python-dotenv` | Loading `.env` |
| `psycopg`, `psycopg-pool`, `psycopg-binary` | PostgreSQL access and connection pools |
| `pydantic` | Parsing JSON configuration (fund strategies) |
| `pandas`, `openpyxl` | Calculations and Excel exports |
| `mailgun` | Admin emails |
| `scikit-learn` | Value/growth style classifier (live and backtesting) |
| `httpx`, `bcrypt` | Web utilities and hashing (`modules/core/util.py`) |

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

The database is the single source of truth: configuration (funds and strategies, benchmarks, the index funds and style reference ETFs), collected data and every result live there. We use the [psycopg](https://www.psycopg.org/) library for connection pool management and CRUD actions, and the [psycopg.rows](https://www.psycopg.org/psycopg3/docs/advanced/rows.html) utility to return rows as classes. Classes are created with the [dataclass](https://www.datacamp.com/tutorial/python-data-classes) decorator – one module per table under `modules/object/` (live) and `modules/bt/object/` (backtesting).

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
| `provider`, `provider_etf` | The actively managed equity ETFs from the SEC list and FMP: their companies, profile, selection status and benchmark |
| `provider_etf_holding` | Their FMP holdings, one row per line per date, with the ticker each line resolved to |
| `ticker` | Companies/listings: identifiers, sector, country, style, ESG, invalid reason, master ticker, company market cap, turnover, verified share count, free float and float factor |
| `ticker_value` | Daily validated price and market cap (USD) |
| `categorize_etf`, `categorize_etf_holding`, `categorize_ticker` | Style reference ETFs, their holdings and constituents with factors |
| `screener_listing` | The weekly FMP large-cap screen: every line with its type, quote and (once registered) ticker |
| `screener_company` | The weekly screen by company (built by the company builder): one row per company with its region and USD market cap |
| `benchmark`, `benchmark_holding` | Benchmark definitions (region, cap, style, minimum market cap) and their weekly weights |
| `best_idea` | Top 10 active-weight ideas per ETF, date and benchmark mode |
| `fund`, `fund_holding`, `fund_holding_change`, `fund_analysis` | Model funds, their holdings, buys/sells and the calculation snapshot |
| `universe_etf`, `universe_etf_holding`, `market_breakpoint` | The index funds (VTI, VEA, VWO), their weekly holdings snapshots with each company's float cap, and the market's size breakpoints per date and coverage |
| `sec_active_etf`, `sec_ncen_filing`, `sec_etf_classification` | The active ETF list (one row per fund, from its latest N-CEN), the N-CEN filings read from EDGAR, and each fund's FMP strategy |
| `old_provider`, `old_provider_etf`, `old_provider_etf_holding` | The hand-configured providers and ETFs and their scraped holdings, kept from before the SEC/FMP tables replaced them (read only by the old-vs-new comparison of `current_sec_active_etfs.py`) |
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
