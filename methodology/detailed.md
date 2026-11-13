# Best Ideas – Investment Methodology

How the model funds are populated and maintained: which managers we listen to, where the data comes from, how a manager's "best ideas" are measured, and how those ideas become a portfolio. It is written for investment professionals; the software side of the same process is in [README.md](../README.md).

Figures quoted "as of" a date are illustrations of the rules at that date, not parameters – the parameters are listed in [Appendix A](#appendix-a--parameter-reference).

---

## Contents

1. [The idea](#1-the-idea)
2. [The process at a glance](#2-the-process-at-a-glance)
3. [Data sources](#3-data-sources)
4. [The manager universe: actively managed equity ETFs](#4-the-manager-universe-actively-managed-equity-etfs)
5. [Holdings](#5-holdings)
6. [Company data and data quality](#6-company-data-and-data-quality)
7. [Market size and the reference benchmarks](#7-market-size-and-the-reference-benchmarks)
8. [Value / growth style](#8-value--growth-style)
9. [Measuring a manager's best ideas](#9-measuring-a-managers-best-ideas)
10. [Building a fund](#10-building-a-fund)
11. [Maintaining a fund: rebalancing and turnover](#11-maintaining-a-fund-rebalancing-and-turnover)
12. [ESG screen](#12-esg-screen)
13. [Audit trail](#13-audit-trail)
14. [Design decisions and known limitations](#14-design-decisions-and-known-limitations)
15. [Glossary](#15-glossary)
- [Appendix A – Parameter reference](#appendix-a--parameter-reference)

---

## 1. The idea

An active manager's portfolio mixes two things: positions held because the manager has a view, and positions held to stay close to a benchmark, control risk or stay diversified. The positions the manager holds **well above their market weight** are the ones they are most willing to be wrong about – their highest-conviction calls, their *best ideas*. Academic work on mutual funds (Cohen, Polk and Silli, *Best Ideas*, updated 2021) found that managers' largest overweights outperformed, while the rest of the portfolio largely did not.

The model funds apply this across the whole population of US-listed actively managed equity ETFs:

1. identify every actively managed equity ETF and keep the diversified, broad-market ones;
2. read each one's holdings every week;
3. for each, measure each holding's **active weight** – its weight in the ETF minus its weight in a market reference – and keep the top overweights as that manager's best ideas;
4. combine the best ideas of all managers into a consensus score – points for each manager's rank, summed – so the stocks that several managers rank highly come first;
5. build a rules-based portfolio from them – by region, size, style, weighting scheme – and rebalance it on a fixed schedule, with turnover controls.

Active ETFs are used rather than mutual funds because they publish their holdings far more often (most daily; we read them weekly – [§5](#5-holdings)), so the ideas are recent rather than a quarter old.

## 2. The process at a glance

| Step | What happens | Cadence |
|------|--------------|---------|
| Manager universe | Every ETF that tells the SEC it is not an index fund; its profile decides whether it is a diversified equity fund that qualifies ([§4](#4-the-manager-universe-actively-managed-equity-etfs)) | Monthly list, rules re-checked daily |
| Holdings | Each qualifying ETF's holdings, every line identified as a company ([§5](#5-holdings)) | Daily (Tue–Sat); most ETFs refresh weekly |
| Company data | Prices, market caps, share-class consolidation, region, validation ([§6](#6-company-data-and-data-quality)) | Daily |
| Size breakpoints | Where "large cap" ends, measured from the total market ([§7](#7-market-size-and-the-reference-benchmarks)) | Weekly |
| Reference benchmarks | US large blend, US large value, US large growth, International large blend ([§7](#7-market-size-and-the-reference-benchmarks)) | Weekly |
| Best ideas | Top 10 active weights per ETF, against two references – the market and the ETF itself ([§9](#9-measuring-a-managers-best-ideas)) | Weekly |
| Funds | Candidate list across all managers → portfolio → buys / sells / weights ([§10](#10-building-a-fund), [§11](#11-maintaining-a-fund-rebalancing-and-turnover)) | Weekly or every two weeks, per fund |

**Weekly timing.** The weekly generation runs on Wednesday on **Tuesday's closing data**: benchmarks, best ideas and funds all use the same close. When Tuesday is an NYSE holiday, the generation moves a day later (Thursday, on Wednesday's close), else a day earlier, staying in the same week. A fund's rebalancing frequency is counted in calendar weeks, so a moved generation never makes a weekly fund skip a week.

## 3. Data sources

| Source | Used for |
|--------|----------|
| **SEC EDGAR – Form N-CEN** | Which ETFs are actively managed: every registered fund's annual report of its own type ([§4](#4-the-manager-universe-actively-managed-equity-etfs)). Read directly from EDGAR, the day after filing. |
| **Financial Modeling Prep (FMP)** | ETF holdings and profiles (asset class, sector weights, sponsor, AUM); company profiles (identifiers – ISIN, CUSIP, SEC CIK – name, domicile, sector, currency, trading status); daily prices and market caps with history; shares outstanding and free float; quarterly financials (share counts); fundamental factors (growth, margins, valuation ratios, yields) for style classification; ESG scores and risk ratings; FX rates; the stock screener (all large companies by exchange); exchange holiday calendars. |
| **Vanguard index funds – VTI, VEA, VWO** (holdings via FMP) | The "whole market": CRSP US Total Market (VTI), FTSE Developed All Cap ex US (VEA) and FTSE Emerging Markets All Cap China A Inclusion (VWO). They hold nearly every listed company at its float-adjusted weight, so they define the size breakpoints and float factors ([§7](#7-market-size-and-the-reference-benchmarks)). |
| **Style reference ETFs** – Vanguard VUG / VTV / VBK / VBR, iShares Morningstar ILCG / ILCV / IMCG / IMCV / ISCG / ISCV (holdings via FMP) | Which companies the index providers (CRSP, Morningstar) classify as value or growth ([§8](#8-value--growth-style)). |

All market data comes from APIs; nothing is scraped. Every figure is converted to **USD at that date's exchange rate**, from the currency the listing actually trades in.

## 4. The manager universe: actively managed equity ETFs

### Which ETFs are active

Every registered fund files Form N-CEN with the SEC once a year and states its own type (Item C.3): exchange-traded fund, index fund, fund of funds, leveraged or inverse, and so on. **An exchange-traded fund that does not call itself an index fund is actively managed.** This is the fund's own legal statement, not a vendor classification, and it holds up in practice: the "smart beta" funds that look active – Goldman's ActiveBeta (GSLC), JPMorgan's quality factor (JQUA), the BetaBuilders range – report themselves as index funds and are excluded.

A fund stays on the list for 15 months after its last filing (it files every 12) unless the filing reports it terminated. The list is refreshed monthly; about 2,500 funds are on it.

### Which active ETFs are equity funds

Each fund on the list is profiled from its FMP data and classified, first rule that applies:

| Classification | Rule | Used? |
|----------------|------|-------|
| Leveraged / inverse | The filing, the asset class, or the name (2x, Bull/Bear, Inverse) | No – exposure is synthetic |
| Fund of funds | The filing says so | No – its holdings are other funds |
| Buffer / defined outcome | Name (buffer, defined outcome, floor…) | No – option overlay, not stock selection |
| Option income | Name (covered call, premium income, buy-write, 0DTE…) | No – returns driven by the option overlay (funds not named as such are caught by the derivatives selection rule below) |
| Fixed income, multi-asset, alternative, commodity, currency | FMP asset class | No |
| **Equity** | Equity asset class (or none) and **at least 75% of holdings in stocks** | **Yes** |

The equity test reads the holdings themselves: cash, currencies, FX forwards, money-market funds, derivatives, bonds, equity-linked notes, private placements and other funds don't count as stock.

### The fund's profile

Each equity fund is profiled from its holdings against our company data and the total-market index funds:

| Measure | How |
|---------|-----|
| **Region** | Share of the fund in US companies (by each company's region – [§6](#6-company-data-and-data-quality)). ≥ 80% US → US; ≤ 20% → International; otherwise Global. |
| **Size** | Each stock large above its market's 70% breakpoint, small below the 90% one, mid between (Morningstar's approach). The fund is *large* with ≥ 70% large stocks, *small* with ≥ 50% small, *mid* with ≥ 50% mid, *smid* with ≥ 70% mid + small, otherwise *all-cap*. |
| **Style** (large funds) | Share of value vs growth companies: ≥ 60% one way, else blend. |
| **Stock count** | Lines matching a stock the total-market index funds hold. |
| **Sector concentration** | FMP's sector weights (else our companies' sectors): the largest sector – for information; the selection's sector rule is measured on the stored holdings (below). |
| **Country concentration** | Our companies' country of domicile: the largest country. |
| **Emerging-market share** | Share held in stocks VWO holds (FTSE's classification: Taiwan, China, India, Brazil emerging; Korea developed). |

### Selection rules

A profiled equity fund feeds the best ideas (**active**) when it passes every rule. The rules keep out funds whose overweights would reflect a mandate rather than stock selection within a broad large-cap market: **derivative strategies, sector and thematic funds, single-country and emerging-market funds, and funds investing mostly outside the large-cap universe the reference benchmarks measure** ([§7](#7-market-size-and-the-reference-benchmarks)). The rules are applied every time holdings are read, so a fund that drifts out of its mandate drops out on its own and returns when it comes back – except a fund that has used derivatives, which never returns.

| Rule | Passes when | Why |
|------|-------------|-----|
| Region | US or International | A Global fund has no single reference market to measure overweights against. |
| Size | Large or all-cap | The funds are built from large caps; small- and mid-cap managers' overweights are measured against a different opportunity set. |
| Number of stocks | 20 to 200 (US), 20 to 250 (International) | Fewer than 20 is a concentrated fund where every holding is an overweight; above the maximum the fund is close to an index and its tilts are small. International managers spread over more markets and names, so their limit is higher. |
| Derivatives | The fund's holdings have **never** shown an option contract or an equity-linked note | Covered-call, option-income and hedged funds hold an ordinary-looking stock portfolio, but their weights follow the option book, not the manager's view of the stocks. Once seen, the fund is excluded permanently: such funds write options intermittently, and a week without contracts doesn't change the strategy. |
| Sector concentration | No sector more than **20 points** above its reference weight | A sector's reference weight is the highest weight it has in any of the most specific reference benchmarks – International large blend, US large value, US large growth. A sector or thematic fund's overweights reflect its mandate, not stock selection. |
| Top country | ≤ 35% (a US fund's top country may be the US) | Single-country international funds (Japan, India, China) – same reason. |
| Emerging markets | International funds ≤ 35% emerging | "International" means developed international; the reference benchmark is developed. |
| Benchmark coverage | ≥ 70% of the fund's stock weight in companies of its region's large-cap blend benchmark to enter; ≥ 65% to stay | A fund mostly outside the benchmark – mid and small caps, frontier markets, private holdings – has overweights the benchmark cannot measure: every holding outside it is an "overweight" by default. |
| Holdings freshness | Latest holdings ≤ 14 days old | Ideas must be current. |
| Duplicate | Only one row per fund | A fund moved to a new trust is briefly listed twice under one ticker; it must count once. |

Anything unknown fails its rule – except the rules measured on the fund's stored holdings (sector concentration, benchmark coverage), which a newly listed fund passes until its first holdings are read; it does not become active before then.

### How the selection rules are measured and set

The rules aim to remove the fund types that don't express stock selection within a broad market, while removing as few ordinary diversified funds as possible. The principles behind them:

- **Behaviour, not names.** Funds are excluded for what they hold, never for what they are called: a fund name is a marketing choice, and the same words ("innovation", "leaders", "quality") are used by thematic and diversified funds alike. The only name rules are in the equity classification above (leveraged, buffer and option-income funds), where the name states a structure rather than a theme, and they are backed by the derivatives rule, which reads the holdings.
- **Derivatives by the holding lines.** A holding line is an option when it carries an expiry date with a strike and a call / put marker – in any of the formats the data vendor uses ("AAPL US 10/16/26 C340", "SPY 11/17/2026 762.29 C", "SPY CALL USD 12/04/2026", "JPMORGAN CHASE + CO NOV26 355 CALL", the exchanges' standard option code "USAR 261016C00020000") – and an equity-linked note when it is named as one ("WELLS FARGO ELN 10/27") or shows a participation level of 20% or more before a maturity date ("Wells Fargo & Co 147.40% 12/15/2026"). Treasury bills, money-market sweeps, currency forwards and rights also carry dates but never a strike, a call / put marker or a note level, so they don't trigger the rule. Every holding set read since a fund entered the universe counts, as does the holding set read for its monthly profile.
- **Sector concentration against the market, by style and region.** A fixed ceiling on the top sector (the earlier rule: 50%) penalises a growth manager for holding what the growth market holds – technology is over half of the US large growth benchmark – while letting through a fund concentrated in a small sector. Measuring each sector against its own reference weight treats every sector alike: a growth manager may hold as much technology as the growth benchmark, a value manager as much finance as the value benchmark, plus the margin, whereas a defence, retail or semiconductor theme exceeds every benchmark's weight in its sector. The reference is the highest weight across the region / style benchmarks rather than the fund's own region or style, so a fund is never penalised for its style label (a "blend" fund tilted to growth is measured as generously as a growth fund). Sector weights are taken from the fund's identified stock holdings, using our companies' sectors – the same classification as the benchmarks.
- **Benchmark coverage as the universe test.** Best ideas are overweights against the region's large-cap benchmarks (in the US, the blend's value and growth halves); a holding outside the blend has no benchmark weight, so it is an overweight by definition. The share of the fund's identified stock weight in companies of that benchmark (each company counted once, across share classes) measures how far the manager invests in the universe the benchmark describes. Thematic and concentrated funds tend to fail it as well, because their themes run into mid and small caps.
- **A band, not a line, for a measure that moves weekly.** Coverage changes with prices, trades and the benchmark's weekly reconstitution, so a fund near a single line would drop in and out. A fund enters at 70% and stays while at 65% or more; the stay level applies only to a fund that entered under the entry level, so every fund first faces 70%.
- **Round levels.** The levels are set in steps of 5 points rather than at the exact point that separates today's funds: a level fitted precisely to one week's pool is fitted to noise and stops working as funds enter and leave. Each level was chosen where it removes the funds of the targeted type – single-country, emerging, sector-concentrated, out-of-universe – with the fewest ordinary diversified funds removed, and was checked against the funds on either side of it.
- **Accepted residue.** Some thematic funds are diversified enough to pass every test – their holdings look like a broad large-cap portfolio with tilts. They are accepted: by the behavioural tests they are indistinguishable from an ordinary manager with a view, and excluding them would take a name rule.

## 5. Holdings

**Frequency and dating.** Holdings are read from FMP every weekday run. FMP refreshes most funds once a week (dated Sunday), some on other days; each holding set is stored under FMP's own date. A fund's holdings are used for up to 14 days.

**Identifying each line.** Every holding line is matched to a company:

- by its symbol, but only when the security behind that symbol agrees with the line's ISIN / CUSIP or name (a holding's symbol can belong to a different security elsewhere);
- else by ISIN;
- else by name, accepting a candidate only when every meaningful word of the shorter name matches the longer one – never on a single shared word.

Lines that aren't stocks are recognised and excluded from the calculation: cash and sweeps, currencies and FX legs, money-market and other funds, derivatives and options, bonds, equity-linked notes (which often carry their issuer's symbol – an ELN on JPMorgan is not JPMorgan stock), rights, warrants and private holdings.

**Duplicate lines.** When a fund lists the same company on several lines (separate lots), they are summed when they imply the same share price; when the prices disagree they are most likely different securities mis-identified as one, and that company is **quarantined** – left out for that fund that week – rather than risk a wrong weight.

## 6. Company data and data quality

### One company, one weight

A company can trade as several listings – share classes (Alphabet GOOGL / GOOG), foreign listings (Alphabet on XETRA), depositary receipts, and home-market lines in two currencies. Each would otherwise be weighted separately and could be bought twice. All listings are grouped into **companies** using the SEC CIK, shared ISINs, matching names in the same domicile, depositary-receipt names and matching market caps (with safeguards against vendor errors, e.g. a wrong ISIN attached to an unrelated listing). A fund's exposure to all of a company's listings is summed, and the company is measured once.

**Company market cap** = the whole company's equity value: every share class, listed or not (as reported by FMP on the company's primary listing). Listings are never summed – each already carries the whole company's cap.

**Primary listing** – the listing that represents the company: an active, ordinary-share line (not preferred, note, unit, depositary or thinly traded line), on its home exchange, then a US listing, then other markets; in its exchange's own currency.

**Region** – a company is **US** or **International** by the first of these that decides:

1. **The index providers' own country assignment** – a company held by the US total-market index fund (CRSP) is US; one held by the developed or emerging ex-US index funds (FTSE) is International. Held by both, the market holding it at least twice the other's float-adjusted value decides (the data vendor occasionally files a foreign stock under a US company's identifiers); a genuine dual holding (Waste Connections, James Hardie) goes to the next rule.
2. **The company's head-office country**, unless it is a tax haven (Bermuda, Cayman Islands, British Virgin Islands, the Channel Islands and similar). This settles the companies the indexes leave out: foreign companies listed only in the US (Arm, Spotify, MercadoLibre, NXP, Nu, Sea are International) and some US ones (partnerships, business development companies, very low-float companies, new listings – US).
3. **The country of incorporation** of an ordinary listing, unless a tax haven.
4. **The listing exchange** – US when the company's main listing is on a US exchange.

So TSMC, ASML, Shopify, PDD, Trip.com and Spotify are International, while Accenture, Medtronic, Linde and Chubb – incorporated abroad, run from the US and held by the US index – are US. Listing on a US exchange alone no longer makes a company US: that rule called PDD, Arm, Spotify and MercadoLibre US. The same rule is used everywhere: fund profiles, benchmarks and fund region filters.

### Validated prices and market caps

Market caps feed every weight, so they are validated rather than taken as quoted:

- **Cross-check against history.** Each day's value is checked against the stored values for the previous three weeks; a disagreement withholds the new value (a vendor revision is recognised and accepted, and so is a share-count update after a filing: market caps up to 15% off with every price agreeing). A listing without a good value for 5 days is excluded, and re-checked every week, so an exclusion lasts only while the disagreement does.
- **Glitch repair.** Vendor market-cap histories occasionally jump by a wrong unit (pence vs pounds) or share count for days or months. A cap that moves threefold in a day without its price moving is a glitch; the listing's current share count arbitrates which side is wrong, and the bad values are repaired at price × the correct share count.
- **Share-count verification.** When the quoted share count and the history disagree (typically after a merger or buyback), a second source – quarterly financials, or the total-market index funds' holdings – decides which is right before the history is rewritten. The index funds hold only the free float, so they can show that a share count is too low (they hold more shares than the history says exist) but never that it is too high; they are used only that way.
- **Index cross-check.** An index fund cannot hold more of a company than exists, so a company the index funds value at ≥ 1.15× our cap is flagged and its share count re-verified before the benchmarks are formed.

### Float

FMP's free float and the total-market index funds' holdings give each company a **float factor** (the investable share of its equity). It is not used to weight anything ([§14](#14-design-decisions-and-known-limitations)), but a company needs **at least 10% float** to count as large cap – S&P's minimum. It also identifies exchange-traded notes and preferreds named like their issuer (zero equity float), which are never treated as the company.

## 7. Market size and the reference benchmarks

### Size is relative to the market

"Large cap" is defined the way the index providers define it – as the **largest companies making up a given share of the market** – rather than at a fixed dollar line, which drifts with markets (a fixed $10B line would today call many mid-cap funds large).

**How the line is drawn.** Each week, for each market:

1. **Take the whole market.** The market is what the Vanguard total-market index funds hold: VTI (CRSP US Total Market) for the US; VEA + VWO (FTSE Developed ex US and FTSE Emerging, China A shares included) for International. Between them they hold nearly every listed company.
2. **Measure each company's investable size.** A company's float-adjusted market value – the value of its shares actually available to investors – is read from the index fund's position in it: the fund holds every company in proportion to its float, so its position, scaled up to the whole market, gives the company's float-adjusted value. Several listings or share classes of one company are added together, and a company held by both markets counts as US. China A shares, which the index holds at only a quarter of their weight, are scaled up to their full size first.
3. **Line the companies up from largest to smallest and keep a running total** of their float-adjusted value, as a share of the whole market's.
4. **The breakpoint at a coverage *c* is the size of the company at which the running total reaches *c*.** For example, the US breakpoint at 93% coverage is the size of the company that takes the cumulative total of the largest US companies to 93% of the US market; every company at least that large is in the top 93%.

The breakpoints therefore move with the market: when markets rise, so does the line, and "large cap" keeps meaning the same slice of the market. Each benchmark uses the breakpoints of its screen date (the latest stored on or before it). As of September 2026:

| Coverage | US | International |
|----------|----|---------------|
| 70% | $94B (122 companies) | $15B (551) |
| 80% | $50B (226) | **$7.4B (983) – International benchmark** |
| 90% | $16B (473) | $3.1B (1,908) |
| 93% | **$10.1B (637) – US benchmark** | $2.2B (2,413) |

- **US large cap**: 93% coverage.
- **International large cap**: 80% coverage.

**Why 93% for the US.** The Russell 1000 – the large-cap index US large-cap managers are most often measured against, alongside the S&P 500, which covers much the same companies – makes up about 93% of the investable US market. Cutting the US benchmark at 93% coverage therefore gives the universe US large-cap managers compare themselves with, and measures their overweights against it.

**Why 80% for International.** The international index providers draw two main lines: MSCI and FTSE call the top 70% of each market *large cap*, and the top 85% *large + mid cap* (MSCI's Standard indexes, such as MSCI EAFE and MSCI ACWI ex US, the benchmarks international large-cap managers usually report against). 80% is the mid point between those two options (85% is about $5B in September 2026; the others are in the table above). It also puts International large cap reasonably close in size to the US line (about $10.1B at 93%). The same 93% coverage would not: the international market is spread over many more companies in many smaller markets, so 93% of it reaches down to about $2B.

**Applying the line to a company.** A company is **large cap** when its **whole market cap** – every share class, listed or not, not only the floating part – is at or above its region's breakpoint, and at least **10% of it floats**. This is the S&P / Russell convention: the line is drawn on the investable market, a company is measured on its whole size, and a minimum float keeps out companies investors can barely buy. To be sure no company that could pass is missed, the weekly screen of listed companies goes down to 80% of the lower of the two breakpoints.

### The reference benchmarks

Four synthetic benchmarks are rebuilt weekly from the full screened universe (every listed company above the screening threshold, across ~40 exchanges):

| Benchmark | Members |
|-----------|---------|
| **US Large Cap Blend** | US companies with whole market cap ≥ the 93% breakpoint and float ≥ 10% |
| **US Large Cap Value** | The blend's value companies |
| **US Large Cap Growth** | The blend's growth companies |
| **International Large Cap Blend** | International companies with whole cap ≥ the 80% breakpoint and float ≥ 10% |

Membership follows the S&P / Russell convention – size measured on the whole company, with a minimum float. **Weights are proportional to the whole company's market cap** – the size the managers themselves see and size positions against ([§14](#14-design-decisions-and-known-limitations)). In the style benchmarks each company belongs wholly to one side and each side is re-weighted to 100%. There is no International style split: the style reference data is US-only, and about a third of the international benchmark has no reliable style.

Each ETF is linked to its region's blend benchmark – in the US, through it, to the blend's value and growth halves, which its holdings are measured against ([§9](#9-measuring-a-managers-best-ideas)).

## 8. Value / growth style

Each company carries one style, **value** or **growth**, used by the style benchmarks and by style-restricted funds. In order of preference:

1. **Index provider classification** – companies held by the style reference ETFs (CRSP via Vanguard, Morningstar via iShares, large / mid / small). Each reference ETF holding a company is one vote for its style; the majority wins, and a tie keeps the existing style (the two families overlap). These are re-read weekly and re-applied daily, so styles follow the index providers' reconstitutions.
2. **Active managers' classification** – a company not in the reference ETFs but held by active ETFs whose own profile is value or growth takes that style.
3. **Statistical classifier** – for the rest (most international companies), a gradient-boosting model trained on the reference-classified companies' fundamentals (size, sector, industry, growth rates, margins, P/E, P/B, yields) predicts value or growth.

## 9. Measuring a manager's best ideas

For every active ETF, each week:

**Inputs.** The ETF's latest holdings (≤ 14 days old), each company's market cap on the holdings date (± 5 days). An ETF where fewer than **90%** of the holdings have a validated market cap is skipped for the week – a partial picture would distort every weight.

**Active weight** for each company the ETF holds:

```
ETF weight        = the ETF's market value in the company (all listings) / the ETF's total stock value
reference weight  = the company's weight in the reference (below)
active weight (Δ) = ETF weight − reference weight
```

**Two references.** The same holdings are measured two ways, and every fund chooses one:

| Reference | Reference weight | What it captures |
|-----------|------------------|------------------|
| **Market** (default) | US companies: the company's weight in the benchmark **of its own style** – value companies against US Large Cap Value, growth companies against US Large Cap Growth (the few with no style against the US blend). International companies: their weight in the International Large Cap Blend. 0 if not a member. | Overweight versus the company's own style peers in the market. A value manager holding a growth mega-cap at below its growth-index weight isn't credited with a conviction it doesn't have, as it would be against the whole US market – and the manager itself needs no style label: each holding is compared with its own style's benchmark, and all of the ETF's active weights are ranked together. |
| **Self** | The company's market cap ÷ the total market cap of the ETF's own holdings – the ETF re-weighted by market cap | Tilt within the manager's own chosen set: which of its stocks it holds more of than size alone would suggest. Independent of any benchmark, and of how the manager chose the names. |

The reference is decided by the **company's** region and style, never by the fund's: a fund's region and style settings only decide what it may buy ([§10](#10-building-a-fund)). Measuring against the whole US large-cap market (the blend) was the earlier default; it was dropped in November 2026 in favour of the style benchmarks, the more meaningful measure of a manager's conviction.

**Selection.** Companies with a positive active weight are ranked from the largest overweight down. An active weight above **20%** is treated as abnormal (a data problem or an extreme concentrated bet) and dropped. A company of **another region than the ETF's** – a US fund's holding in TSMC, Shell or Brookfield – keeps its place in the weights but is never one of that ETF's best ideas: a fund only takes companies of its own region from ETFs of that region ([§10](#10-building-a-fund)), so such an idea could never be bought and would only push a usable one out of the ETF's top 10. Without this rule a US fund's foreign holding, which has no weight in the US benchmark, often ranked #1. The **top 10** per ETF are that manager's best ideas, with their rank (1 = the largest overweight).

**What the market measure implies.** A stock outside the benchmark (below the large-cap line) has a reference weight of zero, so its whole ETF weight is active weight. Large-cap funds therefore apply the size test themselves ([§10](#10-building-a-fund)) so these never enter as new buys.

## 10. Building a fund

### The fund's mandate

Each fund is defined by a strategy:

| Parameter | Options |
|-----------|---------|
| Number of holdings | e.g. 30, 40, 50 |
| Reference for best ideas | Market or self ([§9](#9-measuring-a-managers-best-ideas)) |
| Region | US, International, or Global: split by number of holdings – the **market split** (below) or a fixed one (e.g. 70% US / 30% International) |
| Size | Large (above its region's benchmark line with ≥ 10% float), mid / small (below it), or any |
| Style | Value, growth, any, or a split by number of holdings – the **market split** (US only) or a fixed one (e.g. 60% growth / 40% value) |
| Rank points | The points a company earns for each rank on a manager's list (by default 512 for #1 down to 1 for #10, each rank half the one above – [below](#the-candidate-list)) |
| ESG | Optionally, ESG-qualified companies only ([§12](#12-esg-screen)) |
| Weighting | Equal, or market-cap based ([below](#weighting)) |
| Rebalancing frequency | Every week or every two weeks |
| Restrictions | Optionally specific ETFs only, or specific exchanges |

Every fund draws on **all active ETFs** unless restricted.

**The market split.** A fund can divide its holdings by the market's own weights instead of fixed numbers – between the US and International, and within the US between growth and value. Each part's share is measured on **the same reference benchmarks the best ideas are measured against** ([§7](#7-market-size-and-the-reference-benchmarks)), so a fund is consistent from the measure of a manager's conviction to its own allocation: the US share is the US large-cap benchmark's total over the US and International ones together, the growth share the US growth benchmark's over growth and value. The totals are **float-adjusted** – each company's market cap times the share of it that floats – as the index providers weight their indexes: on whole market caps, International would count the state-, family- and cross-held shares common outside the US in full (58% US on whole caps against 65% float-adjusted, November 2026). The split is recalculated **quarterly**, on the first weekly run of each calendar quarter, and each rebalancing uses the latest one. International has no style split. As of November 2026 the market split was 65.0% US / 35.0% International and 57.5% growth / 42.5% value (US), so a 50-holding global fund holds 33 US and 17 International companies (each share of the holdings rounded to the nearest whole number, half up; the last part takes the rest). When a new split moves a holding from one part to another, nothing is sold to rebalance: the part above its new target makes no buys until its own sells bring it down, and freed positions go to the part below its target.

### The candidate list

1. **Collect** each active ETF's latest best ideas within the fund's look-back (its rebalancing period), in the fund's chosen reference.
2. **Filter** by the mandate. Region applies to both sides: a US fund takes US companies from US-region ETFs; an International fund, International companies from International ETFs. Size, style, exchange and ESG as configured.
3. **Score each company across managers.** Each manager gives a company points for its rank on that manager's best-ideas list, and the points are summed into the company's **consensus score**:

   | Rank | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 |
   |------|---|---|---|---|---|---|---|---|---|----|
   | Points | 512 | 256 | 128 | 64 | 32 | 16 | 8 | 4 | 2 | 1 |

   Each rank is worth half the one above, so one manager's #1 (512) equals two managers' #2 or eight managers' #4. A manager counts once per company. Alongside the score, each company carries its **best rank** with any manager, its **appearances** (how many managers rank it) and its **largest active weight** among them, with the ETF that holds it (the *source ETF*).
4. **Order**: highest score first, then most appearances, then largest active weight. Every manager counts equally, whatever its size or the date of its holdings within the look-back.

So a stock that is the #2 of three managers (768) comes ahead of one that is the #1 of a single manager (512), and a stock ranked #4 to #10 by many managers can outscore one ranked #3 by only one. The ranking deltas within a manager's list, which can be marginal, matter less than how many managers agree.

5. **The ideal portfolio** is built in **buckets**, each ordered on its own: one per region of a region split, and one per style of a style split. Each bucket's ideal holdings are the top of its list, up to its share of the holdings (33 and 17 for a 50-holding fund on the November 2026 market split; with a style split as well, the 33 US positions become 19 growth and 14 value, and the 17 International stay one bucket). With a region split, the International list is built first and those companies are excluded from the US list. With a style split, a company of neither style (rare) is in no bucket.

### Weighting

- **Equal weight**: 1 / number of holdings.
- **Market-cap based**: proportional to the **square root** of each company's market cap – a company 100× larger gets 10× the weight, not 100×, so the fund keeps a size tilt without being dominated by its largest names. Then each position is capped at **10%** and floored at **1%**, the difference redistributed across the others in proportion to their weights.

## 11. Maintaining a fund: rebalancing and turnover

Funds are re-evaluated on the weekly generation day, when their rebalancing period has passed. At each rebalance, starting from the current holdings:

| Current holding | Action |
|-----------------|--------|
| Within the top 1.25 × its bucket's share by score (e.g. the top 63 of a 50-holding fund; 42 US / 22 International at 33 / 17) | **Keep** – a holding that drifts a few places below the cut-off is tolerated, to limit turnover |
| Still scored, but below that line | **Sell** – "below the score buffer" |
| No longer among any manager's best ideas | **Sell** – "no longer a best idea" |

Free positions are then filled from the ideal portfolio in score order (**buys**), bucket by bucket – each bucket's free positions from its own ideas, up to its share, so a sold international holding is replaced by an international idea and the split stays exact. A bucket short of ideas isn't made up from another. A held stock is not replaced merely because a higher-scoring idea has appeared: positions are only freed by sells, so a new idea scoring inside the cut-off can wait outside the fund until a position frees up. That delay is the price of the lower turnover. Weights are then recalculated for the whole portfolio.

Other maintenance rules:

- **A large-cap holding that slips below the large-cap line** is not sold for that reason alone – it stays eligible while it keeps its place by score – but such a stock can never be a new buy.
- **Corporate changes**: holdings are tracked at company level, so a change in a company's primary listing (e.g. after a re-listing) doesn't trigger a sale and re-purchase.
- **No ideas at all** (e.g. a data outage): the fund carries its holdings over unchanged rather than liquidating.

Every buy and sell is recorded with its reason, score, best rank, number of managers and contributing ETFs.

## 12. ESG screen

ESG data comes from FMP (ESG disclosure scores and the ESG risk rating), refreshed weekly. A company is **ESG-qualified** when every available factor passes:

| Factor | Passing |
|--------|---------|
| ESG risk rating | AAA to B |
| ESG score | ≥ 50 |
| Governance score | ≥ 50 |

A company with no ESG data is not qualified. The screen applies only to funds configured as ESG-only.

## 13. Audit trail

Every fund rebalance stores its full calculation: for every ETF the fund could draw on, every company held, its market cap, ETF weight, reference weight (and which benchmark it came from, on which date), active weight, rank, and why it did or didn't become a best idea (selected; another region than the ETF's; not overweight; above the 20% limit; beyond the top 10; no validated market cap; quarantined). This is recomputed independently and cross-checked against the stored best ideas.

For any fund and date, a **methodology export** produces a set of spreadsheets tracing the fund from the ground up: each ETF's holdings and active weights, the benchmark, the combined best ideas, and the fund's holdings, buys and sells.

A **simulation** replays the same rules week by week over the holdings collected so far, using each date's own market caps and benchmarks, to show how a fund would have been built and changed from a chosen start date.

## 14. Design decisions and known limitations

**Design decisions**

- **Whole-company cap, not float-adjusted, for benchmark weights.** Managers size positions against a company's full size; a float-adjusted benchmark would make closely held companies look like overweights for every manager. A float-weighted version was tested and reverted. Float is used only as a minimum (10%) for membership.
- **Market-relative size lines** rather than fixed dollar thresholds, so "large cap" means the same thing in every market environment.
- **Consensus before single conviction.** A company's score adds up every manager that ranks it, each rank worth half the one above, so agreement across managers weighs as much as the strength of one manager's view, and marginal differences between neighbouring ranks matter less. Ranking by best rank alone (the earlier rule) let one manager's #1 outrank ten managers' #2 and turned small active-weight differences into portfolio changes.
- **Managers selected by holdings behaviour.** Funds are kept or excluded by what they hold – derivatives, sector concentration against the benchmarks, country and emerging-market exposure, benchmark coverage – never by their name, at round levels rather than levels fitted to the funds of the day ([§4](#how-the-selection-rules-are-measured-and-set)).
- **Every manager counts equally**, regardless of assets under management or holdings date – the method aggregates opinions, not money.
- **The market split from our own benchmarks.** Measuring the split on the benchmarks the best ideas are measured against keeps the fund consistent across region and style, and it follows the market every quarter without a manual input. It's our definition – our large-cap lines, float factors from the index funds and the data vendor – not a published index weight. The float factors are current ones: older splits can't be re-measured on the float of their day.
- **US companies measured against their own style.** A manager holding both styles is measured against two benchmarks in which large companies weigh more than in the blend (×2.3 for value, ×1.75 for growth), so its largest holdings rank somewhat lower than against the whole US market. Accepted, in exchange for not crediting out-of-style mega-caps as convictions.
- **Reference and mandate kept apart.** Which benchmark a company is measured against follows the company's own style; the fund's style setting – a filter or a value / growth split of its holdings – is a separate construction choice. A fund without a style split still measures its US ideas against their style peers; a fund with one fills each style from the ideas measured that way.

**Limitations**

- **Holdings lag.** Although active ETFs publish daily, the data vendor refreshes most funds weekly; ideas are typically several days old, and up to two weeks, when used.
- **New funds join late.** A fund appears on the active list only after its trust's first N-CEN filing after launch (up to a year), and the list is refreshed monthly.
- **Active ETFs only.** Mutual funds and separately managed accounts, which hold most active assets, are not covered; the manager population is younger and skewed toward sponsors that have launched ETFs.
- **Vendor data.** Holdings, prices, share counts, style factors and ESG come from one vendor (FMP); the validation in [§6](#6-company-data-and-data-quality) catches known failure modes but cannot rule out every error. Lines that cannot be identified (a small share of weight, mostly emerging-market names) take no part.
- **No International style split** – International value / growth funds rely on style classification without an International style benchmark.
- **Weighting is heuristic.** Square-root market-cap weighting with a 10% cap / 1% floor is a rule, not an optimisation; no risk model, transaction costs or liquidity constraints are applied.
- **No performance claim.** This document describes how the funds are constructed; it makes no claim about their future performance.

## 15. Glossary

| Term | Meaning |
|------|---------|
| **Active weight (Δ)** | A company's weight in the ETF minus its weight in the reference. |
| **Appearances** | Number of managers ranking a company among their best ideas. |
| **Best idea** | One of the top 10 positive active weights of an ETF in a week. |
| **Market split** | A fund's division of its holdings by the float-adjusted weights of the reference benchmarks (US / International; growth / value within the US), recalculated quarterly. |
| **Bucket** | A region (and, with a style split, a style) of a fund, filled from its own ordered list up to its share of the holdings. |
| **Breakpoint** | The company size at which the market's largest companies reach a given share of total market value. |
| **Company market cap** | The whole company's equity value, all share classes, in USD. |
| **Float factor** | The investable share of a company's equity, from the index funds' holdings or free float. |
| **Primary listing** | The listing that represents a company: its home-market ordinary shares. |
| **Rank** | A company's position in a manager's best-ideas list (1 = largest overweight). |
| **Consensus score** | The sum of the rank points a company earns from every manager ranking it. |
| **Source ETF** | The ETF with the largest active weight in a fund holding. |

---

## Appendix A – Parameter reference

| Parameter | Value |
|-----------|-------|
| Active ETF: SEC list window | Last filing within 15 months |
| Equity fund: minimum stock share | 75% |
| Fund region: US / International | ≥ 80% / ≤ 20% US |
| Company region: held by both index markets | One market's float-adjusted holding ≥ 2× the other's decides |
| Fund style: value / growth | ≥ 60% of style-classified weight |
| Selection: stocks | 20–200 (US) / 20–250 (International) |
| Selection: derivatives | Excluded for good once holdings show an option or an equity-linked note (note level ≥ 20%) |
| Selection: sector concentration | No sector > 20 points above its highest weight in the International blend / US value / US growth benchmarks |
| Selection: top country | ≤ 35% (US funds: the US exempt) |
| Selection: International emerging share | ≤ 35% |
| Selection: benchmark coverage | ≥ 70% to enter / ≥ 65% to stay |
| Holdings: maximum age | 14 days |
| Market cap: validation window | 21 days; excluded after 5 days without a good value |
| Large cap: US / International coverage | 93% / 80% of the market |
| Large cap: minimum float | 10% |
| Fund profile size lines | 70% (large) / 90% (small) |
| Best ideas: minimum priced holdings | 90% |
| Best ideas: market cap window | ± 5 days of the holdings date |
| Best ideas: maximum active weight | 20% |
| Best ideas: per ETF | 10 |
| Fund: rank points, ranks 1–10 | 512, 256, 128, 64, 32, 16, 8, 4, 2, 1 |
| Fund: market split (US / International; growth / value, US) | Float-adjusted reference benchmark totals, recalculated quarterly – 65.0% / 35.0%; 57.5% / 42.5% (November 2026) |
| Fund: keep buffer | Top 1.25 × the bucket's share of holdings, by score |
| Fund weighting: exponent / cap / floor | 0.5 (square root) / 10% / 1% |
| ESG: risk rating / ESG score / governance | AAA–B / ≥ 50 / ≥ 50 |
| Weekly generation | Wednesday on Tuesday's close (moved around NYSE holidays) |
