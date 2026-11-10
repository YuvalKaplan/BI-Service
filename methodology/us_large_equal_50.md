# US Large Cap Best Ideas – Equal Weight, 50 Holdings

How this fund is populated and maintained: which managers it listens to, where the data comes from, how each manager's "best ideas" are measured, and how those ideas become a 50-stock, equally weighted portfolio of US large-cap companies. It is written for investment professionals.

Figures quoted "as of" a date illustrate the rules at that date; they are not parameters. The parameters are listed in [Appendix A](#appendix-a--parameter-reference).

---

## Contents

1. [The idea](#1-the-idea)
2. [The fund at a glance](#2-the-fund-at-a-glance)
3. [Data sources](#3-data-sources)
4. [The managers: actively managed US equity ETFs](#4-the-managers-actively-managed-us-equity-etfs)
5. [Holdings](#5-holdings)
6. [Company data and data quality](#6-company-data-and-data-quality)
7. [US large cap and the value / growth benchmarks](#7-us-large-cap-and-the-value--growth-benchmarks)
8. [Value / growth style](#8-value--growth-style)
9. [Measuring a manager's best ideas](#9-measuring-a-managers-best-ideas)
10. [Building the portfolio](#10-building-the-portfolio)
11. [Maintaining the portfolio: rebalancing and turnover](#11-maintaining-the-portfolio-rebalancing-and-turnover)
12. [Audit trail](#12-audit-trail)
13. [Design decisions and known limitations](#13-design-decisions-and-known-limitations)
14. [Glossary](#14-glossary)
- [Appendix A – Parameter reference](#appendix-a--parameter-reference)

---

## 1. The idea

An active manager's portfolio mixes two things: positions held because the manager has a view, and positions held to stay close to a benchmark, control risk or stay diversified. The positions the manager holds **well above their market weight** are the ones they are most willing to be wrong about – their highest-conviction calls, their *best ideas*. Academic work on mutual funds (Cohen, Polk and Silli, *Best Ideas*, updated 2021) found that managers' largest overweights outperformed, while the rest of the portfolio largely did not.

The fund applies this across the population of US-listed, actively managed, diversified US equity ETFs:

1. identify every actively managed US equity ETF with a broad, diversified mandate;
2. read each one's holdings every week;
3. for each, measure each holding's **active weight** – its weight in the ETF minus its weight in its style's US large-cap benchmark – and keep the top overweights as that manager's best ideas;
4. combine the best ideas of all managers, preferring the stocks that rank highest for any manager and that several managers share;
5. hold the top 50 US large-cap companies at equal weight, and rebalance every two weeks with turnover controls.

Active ETFs are used rather than mutual funds because they publish their holdings far more often (most daily; we read them weekly – [§5](#5-holdings)), so the ideas are recent rather than a quarter old.

## 2. The fund at a glance

| | |
|---|---|
| **Universe** | US large-cap companies |
| **Source of ideas** | Every qualifying actively managed US equity ETF |
| **Best ideas measured against** | The US Large Cap Value benchmark for value companies, the US Large Cap Growth benchmark for growth companies |
| **Ideas used** | Each manager's top three |
| **Style** | No constraint: value and growth ideas compete on rank alone |
| **Holdings** | 50 |
| **Weighting** | Equal (2% each) |
| **Rebalancing** | Every two weeks, with turnover controls |

**Weekly timing.** Benchmarks and best ideas are rebuilt every Wednesday on **Tuesday's closing data**, and the fund is rebalanced on every second such day. All of them use the same close. When Tuesday is an NYSE holiday, the weekly run moves a day later (Thursday, on Wednesday's close), else a day earlier, staying in the same week. The two-week rebalancing period is counted in calendar weeks, so a moved run never makes the fund skip a rebalance.

| Step | What happens | Cadence |
|------|--------------|---------|
| Managers | Every ETF that tells the SEC it is not an index fund; its profile decides whether it is a diversified US equity fund that qualifies ([§4](#4-the-managers-actively-managed-us-equity-etfs)) | Monthly list, rules re-checked daily |
| Holdings | Each qualifying ETF's holdings, every line identified as a company ([§5](#5-holdings)) | Daily (Tue–Sat); most ETFs refresh weekly |
| Company data | Prices, market caps, share-class consolidation, validation ([§6](#6-company-data-and-data-quality)) | Daily |
| Large-cap line and benchmarks | Where US large cap ends; the value and growth benchmarks ([§7](#7-us-large-cap-and-the-value--growth-benchmarks)) | Weekly |
| Best ideas | Top 10 active weights per ETF ([§9](#9-measuring-a-managers-best-ideas)) | Weekly |
| Portfolio | Candidate list across all managers → 50 holdings → buys and sells ([§10](#10-building-the-portfolio), [§11](#11-maintaining-the-portfolio-rebalancing-and-turnover)) | Every two weeks |

## 3. Data sources

| Source | Used for |
|--------|----------|
| **SEC EDGAR – Form N-CEN** | Which ETFs are actively managed: every registered fund's annual report of its own type ([§4](#4-the-managers-actively-managed-us-equity-etfs)). Read directly from EDGAR, the day after filing. |
| **Financial Modeling Prep (FMP)** | ETF holdings and profiles (asset class, sector weights, sponsor, AUM); company profiles (identifiers – ISIN, CUSIP, SEC CIK – name, domicile, sector, trading status); daily prices and market caps with history; shares outstanding and free float; quarterly financials (share counts); fundamental factors (growth, margins, valuation ratios, yields) for style classification; the stock screener (all large companies); NYSE holiday calendar. |
| **Vanguard Total Stock Market ETF – VTI** (holdings via FMP) | The whole US market: the CRSP US Total Market index holds nearly every listed US company at its float-adjusted weight, so it defines where large cap ends and each company's float ([§7](#7-us-large-cap-and-the-value--growth-benchmarks)). |
| **Style reference ETFs** – Vanguard VUG / VTV / VBK / VBR, iShares Morningstar ILCG / ILCV / IMCG / IMCV / ISCG / ISCV (holdings via FMP) | Which companies the index providers (CRSP, Morningstar) classify as value or growth ([§8](#8-value--growth-style)). |

All market data comes from APIs; nothing is scraped.

## 4. The managers: actively managed US equity ETFs

### Which ETFs are active

Every registered fund files Form N-CEN with the SEC once a year and states its own type (Item C.3): exchange-traded fund, index fund, fund of funds, leveraged or inverse, and so on. **An exchange-traded fund that does not call itself an index fund is actively managed.** This is the fund's own legal statement, not a vendor classification, and it holds up in practice: the "smart beta" funds that look active – Goldman's ActiveBeta (GSLC), JPMorgan's quality factor (JQUA), the BetaBuilders range – report themselves as index funds and are excluded.

A fund stays on the list for 15 months after its last filing (it files every 12) unless the filing reports it terminated. The list is refreshed monthly.

### Which active ETFs are equity funds

Each fund on the list is profiled from its FMP data and classified, first rule that applies:

| Classification | Rule | Used? |
|----------------|------|-------|
| Leveraged / inverse | The filing, the asset class, or the name (2x, Bull/Bear, Inverse) | No – exposure is synthetic |
| Fund of funds | The filing says so | No – its holdings are other funds |
| Buffer / defined outcome | Name (buffer, defined outcome, floor…) | No – option overlay, not stock selection |
| Option income | Name (covered call, premium income, buy-write, 0DTE…) | No – returns driven by the option overlay |
| Fixed income, multi-asset, alternative, commodity, currency | FMP asset class | No |
| **Equity** | Equity asset class (or none) and **at least 75% of holdings in stocks** | **Yes** |

The equity test reads the holdings themselves: cash, currencies, FX forwards, money-market funds, derivatives, bonds, equity-linked notes, private placements and other funds don't count as stock.

### The fund's profile

Each equity fund is profiled from its holdings against our company data and the total US market:

| Measure | How |
|---------|-----|
| **US share** | Share of the fund in US companies (by each company's primary listing – [§6](#6-company-data-and-data-quality)). |
| **Size** | Each stock large above the US market's 70% breakpoint, small below the 90% one, mid between (Morningstar's approach, [§7](#7-us-large-cap-and-the-value--growth-benchmarks)). The fund is *large* with ≥ 70% large stocks, *small* with ≥ 50% small, *mid* with ≥ 50% mid, *smid* with ≥ 70% mid + small, otherwise *all-cap*. |
| **Stock count** | Lines matching a stock the total-market index fund holds. |
| **Sector concentration** | FMP's sector weights (else our companies' sectors): the largest sector. |

### Selection rules

A profiled equity fund is one of the fund's managers (**active**) when it passes every rule. These are applied daily, so a manager that drifts out of its mandate drops out on its own, and returns when it comes back:

| Rule | Passes when | Why |
|------|-------------|-----|
| US | At least 80% of the fund in US companies | The managers' overweights are measured against the US market. |
| Size | Large or all-cap | Small- and mid-cap managers' overweights are measured against a different opportunity set. |
| Number of stocks | 20 to 200 | Fewer than 20 is a concentrated fund where every holding is an overweight; above 200 the fund is close to an index and its tilts are small. |
| Top sector | ≤ 50% of the fund | A sector fund's overweights reflect its mandate, not stock selection. |
| Holdings freshness | Latest holdings ≤ 14 days old | Ideas must be current. |
| Duplicate | Only one row per fund | A fund moved to a new trust is briefly listed twice under one ticker; it must count once. |

Anything unknown fails its rule.

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

A company can trade as several listings – share classes (Alphabet GOOGL / GOOG), foreign listings (Alphabet on XETRA) and depositary receipts. Each would otherwise be weighted separately and could be bought twice. All listings are grouped into **companies** using the SEC CIK, shared ISINs, matching names, depositary-receipt names and matching market caps (with safeguards against vendor errors, e.g. a wrong ISIN attached to an unrelated listing). A manager's exposure to all of a company's listings is summed, and the company is measured and held once.

**Company market cap** = the whole company's equity value: every share class, listed or not (as reported by FMP on the company's primary listing). Listings are never summed – each already carries the whole company's cap.

**Primary listing** – the listing that represents the company: an active, ordinary-share line (not preferred, note, unit, depositary or thinly traded line), on its home exchange, then a US listing, then other markets.

**US company** – a company whose primary listing is on NYSE / NASDAQ / AMEX. Eaton, Medtronic, Linde and Arm are US companies; TSMC, ASML, SAP, Shell and Shopify, whose home listings are abroad, are not, even though US managers often hold their US-listed shares. The same rule is used everywhere: manager profiles, the benchmarks and the fund.

### Validated prices and market caps

Market caps decide which companies are large cap and feed every benchmark weight, so they are validated rather than taken as quoted:

- **Cross-check against history.** Each day's value is checked against the stored values for the previous three weeks; a disagreement withholds the new value (a vendor revision is recognised and accepted, and so is a share-count update after a filing: market caps up to 15% off with every price agreeing). A listing without a good value for 5 days is excluded, and re-checked every week, so an exclusion lasts only while the disagreement does.
- **Glitch repair.** Vendor market-cap histories occasionally jump by a wrong share count for days or months. A cap that moves threefold in a day without its price moving is a glitch; the listing's current share count arbitrates which side is wrong, and the bad values are repaired at price × the correct share count.
- **Share-count verification.** When the quoted share count and the history disagree (typically after a merger or buyback), a second source – quarterly financials, or the total-market index fund's holdings – decides which is right before the history is rewritten. The index fund holds only the free float, so it can show that a share count is too low (it holds more shares than the history says exist) but never that it is too high; it is used only that way.
- **Index cross-check.** An index fund cannot hold more of a company than exists, so a large-cap company the index fund values at ≥ 1.15× our cap is flagged and its share count re-verified before the benchmarks are formed.

### Float

FMP's free float and the total-market index fund's holdings give each company a **float factor** (the investable share of its equity). It is not used to weight anything ([§13](#13-design-decisions-and-known-limitations)), but a company needs **at least 10% float** to count as large cap – S&P's minimum. It also identifies exchange-traded notes and preferreds named like their issuer (zero equity float), which are never treated as the company.

## 7. US large cap and the value / growth benchmarks

### Where large cap ends

"Large cap" is defined the way the index providers define it – as the **largest companies making up a given share of the market** – rather than at a fixed dollar line, which drifts with markets (a fixed $10B line would today call many mid-cap funds large).

**How the line is drawn.** Each week:

1. **Take the whole market** – what the Vanguard Total Stock Market ETF (VTI, CRSP US Total Market) holds: nearly every listed US company.
2. **Measure each company's investable size.** A company's float-adjusted market value – the value of its shares actually available to investors – is read from VTI's position in it: the fund holds every company in proportion to its float, so its position, scaled up to the whole market, gives the company's float-adjusted value. Several listings or share classes of one company are added together.
3. **Line the companies up from largest to smallest and keep a running total** of their float-adjusted value, as a share of the whole market's.
4. **The breakpoint is the size of the company at which the running total reaches 93%** – the share of the US market the Russell 1000 represents. Every company at least that large is in the top 93% of the market: about $10.1B in September 2026.

The line therefore moves with the market: when markets rise, so does the line, and "large cap" keeps meaning the same slice of the market. The same running total gives the 70% and 90% lines ($94B and $16B), used only to size the managers ([§4](#4-the-managers-actively-managed-us-equity-etfs)).

**Applying the line to a company.** A company is **large cap** when its **whole market cap** – every share class, listed or not, not only the floating part – is at or above the breakpoint, and at least **10% of it floats**. This is the S&P / Russell convention: the line is drawn on the investable market, a company is measured on its whole size, and a minimum float keeps out companies investors can barely buy. To be sure no company that could pass is missed, the weekly screen of listed companies goes down to 80% of the line.

### The value and growth benchmarks

Two benchmarks are rebuilt weekly from the full screened US universe (every listed US company above the screening threshold):

| Benchmark | Members |
|-----------|---------|
| **US Large Cap Value** | The US large-cap companies classified value ([§8](#8-value--growth-style)) |
| **US Large Cap Growth** | The US large-cap companies classified growth |

Each company belongs wholly to one side. Within each benchmark, **weights are proportional to the whole company's market cap** – the size the managers themselves see and size positions against ([§13](#13-design-decisions-and-known-limitations)) – and add up to 100%.

## 8. Value / growth style

Each company carries one style, **value** or **growth**, which decides the benchmark it is measured against. In order of preference:

1. **Index provider classification** – companies held by the style reference ETFs (CRSP via Vanguard, Morningstar via iShares, large / mid / small). Each reference ETF holding a company is one vote for its style; the majority wins, and a tie keeps the existing style (the two families overlap). These are re-read weekly and re-applied daily, so styles follow the index providers' reconstitutions.
2. **Active managers' classification** – a company not in the reference ETFs but held by active ETFs whose own profile is value or growth (at least 60% of their style-classified holdings) takes that style.
3. **Statistical classifier** – for the rest, a gradient-boosting model trained on the reference-classified companies' fundamentals (size, sector, industry, growth rates, margins, P/E, P/B, yields) predicts value or growth.

## 9. Measuring a manager's best ideas

For every active ETF, each week:

**Inputs.** The ETF's latest holdings (≤ 14 days old), each company's market cap on the holdings date (± 5 days). An ETF where fewer than **90%** of the holdings have a validated market cap is skipped for the week – a partial picture would distort every weight.

**Active weight.** For each company the ETF holds, two weights are compared:

- **the manager's weight** – the share of the ETF's stock portfolio invested in the company, across all its listings;
- **the benchmark weight** – the company's share of its own style's benchmark: US Large Cap Value for a value company, US Large Cap Growth for a growth company. A company below the large-cap line is in neither, so its benchmark weight is zero.

The **active weight** is the difference: how much more of the company the manager holds than the benchmark does. For example, a manager with 4% of its fund in a growth company that makes up 1% of the growth benchmark has an active weight of 3 percentage points – a clear overweight. A company held at or below its benchmark weight is not an overweight at all.

All of a manager's active weights, value and growth companies alike, are then ranked together.

**Why each company is measured against its own style.** A manager's overweight is most meaningful against the peers it competes with for a place in the portfolio. Measured against the whole large-cap market, a value manager that holds a growth mega-cap at well below its growth-index weight could still show it as one of its largest overweights – a conviction it doesn't have. Against the growth benchmark, where that company weighs more, it shows as what it is. The manager itself needs no style label: each of its holdings is simply compared with its own style's benchmark. (The few large-cap companies with no style – about 0.1% of the market – are measured against the whole US large-cap market.)

**Selection.** Companies with a positive active weight are ranked from the largest overweight down. An active weight above **20%** is treated as abnormal (a data problem or an extreme concentrated bet) and dropped. A **non-US company** – a US manager's holding in TSMC, Shell or Brookfield – counts in the manager's total, but is never one of its best ideas: the fund holds only US companies, so such an idea could never be bought and would only push a usable one out of the manager's list. The **top 10** per ETF are that manager's best ideas, with their rank (1 = the largest overweight).

**Companies below the large-cap line.** Such a company has no weight in either benchmark, so its whole ETF weight is active weight. The fund applies the large-cap test itself ([§10](#10-building-the-portfolio)), so these never enter as new buys.

## 10. Building the portfolio

### The candidate list

1. **Collect** each active ETF's latest best ideas from the last two weeks (the rebalancing period).
2. **Filter**: US companies only, from US ETFs, and only those that are large cap on the rebalancing date – their whole market cap at or above the US large-cap breakpoint, with at least 10% float ([§7](#7-us-large-cap-and-the-value--growth-benchmarks)).
3. **Combine across managers**, per company:
   - its **best rank** with any manager;
   - its **appearances** – how many managers rank it at that best rank;
   - its **largest active weight** among them, and the ETF that holds it (the *source ETF*).
4. **Order**: best rank first, then most appearances, then largest active weight. Every manager counts equally, whatever its size or the date of its holdings within the two weeks.

So a stock that is the #1 overweight of four managers comes ahead of one that is the #1 of one manager, which comes ahead of anyone's #2.

5. **The ideal portfolio** is the top 50 of this list among the companies ranked **#1 to #3** by at least one manager. Value and growth companies compete on the same list; there is no style quota.

### Weighting

Every holding is held at **equal weight**: 1/50, or 2%. If fewer than 50 eligible ideas exist on a rebalancing date, the fund holds what there is, still equally weighted.

## 11. Maintaining the portfolio: rebalancing and turnover

The fund is rebalanced every two weeks, on the weekly run. Starting from the current holdings:

| Current holding | Action |
|-----------------|--------|
| Still in the ideal portfolio | **Keep** |
| No longer ranked #5 or better by any manager | **Sell** – "not in best ideas" |
| Still ranked, but its best rank has worsened by 2 or more since it was bought | **Sell** – "dropped in ranking" |
| Still ranked, worsened by less | **Keep** – tolerated to limit turnover |

Free positions are then filled from the ideal portfolio in order (**buys**), up to 50. A held stock is not replaced merely because a better-ranked idea has appeared: positions are only freed by sells. All holdings are then reset to equal weight.

Other maintenance rules:

- **A holding that slips below the large-cap line** is not sold for that reason alone – it stays eligible while managers keep ranking it – but such a stock can never be a new buy.
- **Corporate changes**: holdings are tracked at company level, so a change in a company's primary listing (e.g. after a re-listing) doesn't trigger a sale and re-purchase.
- **No ideas at all** (e.g. a data outage): the fund carries its holdings over unchanged rather than liquidating.

Every buy and sell is recorded with its reason, rank, number of managers and contributing ETFs.

## 12. Audit trail

Every rebalance stores its full calculation: for every ETF the fund could draw on, every company held, its market cap, ETF weight, reference weight (and which benchmark – value or growth – it came from, on which date), active weight, rank, and why it did or didn't become a best idea (selected; not a US company; not overweight; above the 20% limit; beyond the top 10; no validated market cap; quarantined). This is recomputed independently and cross-checked against the stored best ideas.

For any rebalancing date, a **methodology export** produces a set of spreadsheets tracing the fund from the ground up: each ETF's holdings and active weights, the benchmarks, the combined best ideas, and the fund's holdings, buys and sells.

A **simulation** replays the same rules week by week over the holdings collected so far, using each date's own market caps and benchmarks, to show how the fund would have been built and changed from a chosen start date.

## 13. Design decisions and known limitations

**Design decisions**

- **Each company against its own style's benchmark.** Out-of-style mega-caps aren't credited as convictions ([§9](#9-measuring-a-managers-best-ideas)). The trade-off: a manager that holds both styles is measured against two benchmarks in which large companies weigh more than in the whole large-cap market (×2.3 for value, ×1.75 for growth), so its largest holdings rank somewhat lower. Accepted.
- **Whole-company cap, not float-adjusted, for benchmark weights.** Managers size positions against a company's full size; a float-adjusted benchmark would make closely held companies look like overweights for every manager. A float-weighted version was tested and reverted. Float is used only as a minimum (10%) for large-cap membership.
- **A market-relative large-cap line** rather than a fixed dollar threshold, so "large cap" means the same thing in every market environment.
- **Rank before breadth.** A company's best rank with any manager outweighs how many managers hold it; breadth breaks ties. This favours each manager's single strongest conviction over consensus names.
- **Every manager counts equally**, regardless of assets under management or holdings date – the method aggregates opinions, not money.
- **Equal weight.** Each idea gets the same conviction in the fund, whatever the company's size; the fund carries no market-cap tilt.

**Limitations**

- **Holdings lag.** Although active ETFs publish daily, the data vendor refreshes most funds weekly; ideas are typically several days old, and up to two weeks, when used.
- **New funds join late.** A fund appears on the active list only after its trust's first N-CEN filing after launch (up to a year), and the list is refreshed monthly.
- **Active ETFs only.** Mutual funds and separately managed accounts, which hold most active assets, are not covered; the manager population is younger and skewed toward sponsors that have launched ETFs.
- **Vendor data.** Holdings, prices, share counts and style factors come from one vendor (FMP); the validation in [§6](#6-company-data-and-data-quality) catches known failure modes but cannot rule out every error. Lines that cannot be identified (a small share of weight) take no part.
- **Style is a single label.** Each company is wholly value or wholly growth; companies near the boundary can change side when the index providers reconstitute, which changes the benchmark they are measured against.
- **No risk model.** Equal weighting is a rule, not an optimisation; no sector limits, risk model, transaction costs or liquidity constraints are applied.
- **No performance claim.** This document describes how the fund is constructed; it makes no claim about its future performance.

## 14. Glossary

| Term | Meaning |
|------|---------|
| **Active weight (Δ)** | A company's weight in the ETF minus its weight in its style's benchmark. |
| **Appearances** | Number of managers ranking a company at its best rank. |
| **Best idea** | One of the top 10 positive active weights of an ETF in a week. |
| **Breakpoint** | The company size at which the market's largest companies reach a given share of total market value. |
| **Company market cap** | The whole company's equity value, all share classes, in USD. |
| **Float factor** | The investable share of a company's equity, from the index fund's holdings or free float. |
| **Primary listing** | The listing that represents a company: its home-market ordinary shares. |
| **Rank** | A company's position in a manager's best-ideas list (1 = largest overweight). |
| **Source ETF** | The ETF with the largest active weight in a fund holding. |

---

## Appendix A – Parameter reference

| Parameter | Value |
|-----------|-------|
| Active ETF: SEC list window | Last filing within 15 months |
| Equity fund: minimum stock share | 75% |
| Manager: minimum US share | 80% |
| Manager: size | Large or all-cap |
| Manager: number of stocks | 20–200 |
| Manager: top sector | ≤ 50% |
| Manager style (for company style, step 2) | ≥ 60% of style-classified holdings |
| Holdings: maximum age | 14 days |
| Market cap: validation window | 21 days; excluded after 5 days without a good value |
| Large cap: US market coverage | 93% |
| Large cap: minimum float | 10% |
| Manager size lines | 70% (large) / 90% (small) |
| Best ideas: minimum priced holdings | 90% |
| Best ideas: market cap window | ± 5 days of the holdings date |
| Best ideas: maximum active weight | 20% |
| Best ideas: per ETF | 10 |
| Fund: holdings | 50 |
| Fund: ideas used | Each manager's ranks 1–3 |
| Fund: weighting | Equal (2%) |
| Fund: rebalancing | Every two weeks |
| Fund: kept while ranked | #5 or better |
| Fund: sell on rank drop | 2 ranks or more since bought |
| Weekly run | Wednesday on Tuesday's close (moved around NYSE holidays) |
