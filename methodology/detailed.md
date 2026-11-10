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

An active manager's portfolio mixes two things: positions held because the manager has a view, and positions held to stay close to a benchmark, control risk or stay diversified. The positions the manager holds **well above their market weight** are the ones they are most willing to be wrong about – their highest-conviction calls, their *best ideas*. Academic work on mutual funds (Cohen, Polk and Silli, *Best Ideas*, 2010) found that managers' largest overweights outperformed, while the rest of the portfolio largely did not.

The model funds apply this across the whole population of US-listed actively managed equity ETFs:

1. identify every actively managed equity ETF and keep the diversified, broad-market ones;
2. read each one's holdings every week;
3. for each, measure each holding's **active weight** – its weight in the ETF minus its weight in a market reference – and keep the top overweights as that manager's best ideas;
4. combine the best ideas of all managers, preferring the stocks that rank highest for any manager and that several managers share;
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
| Best ideas | Top 10 active weights per ETF, against three different references ([§9](#9-measuring-a-managers-best-ideas)) | Weekly |
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
| Option income | Name (covered call, premium income, buy-write, 0DTE…) | No – returns driven by the option overlay |
| Fixed income, multi-asset, alternative, commodity, currency | FMP asset class | No |
| **Equity** | Equity asset class (or none) and **at least 75% of holdings in stocks** | **Yes** |

The equity test reads the holdings themselves: cash, currencies, FX forwards, money-market funds, derivatives, bonds, equity-linked notes, private placements and other funds don't count as stock.

### The fund's profile

Each equity fund is profiled from its holdings against our company data and the total-market index funds:

| Measure | How |
|---------|-----|
| **Region** | Share of the fund in US companies (by each company's primary listing – [§6](#6-company-data-and-data-quality)). ≥ 80% US → US; ≤ 20% → International; otherwise Global. |
| **Size** | Each stock large above its market's 70% breakpoint, small below the 90% one, mid between (Morningstar's approach, [§7](#7-market-size-and-the-reference-benchmarks)). The fund is *large* with ≥ 70% large stocks, *small* with ≥ 50% small, *mid* with ≥ 50% mid, *smid* with ≥ 70% mid + small, otherwise *all-cap*. |
| **Style** (large funds) | Share of value vs growth companies: ≥ 60% one way, else blend. |
| **Stock count** | Lines matching a stock the total-market index funds hold. |
| **Sector concentration** | FMP's sector weights (else our companies' sectors): the largest sector. |
| **Country concentration** | Our companies' country of domicile: the largest country. |
| **Emerging-market share** | Share held in stocks VWO holds (FTSE's classification: Taiwan, China, India, Brazil emerging; Korea developed). |

### Selection rules

A profiled equity fund feeds the best ideas (**active**) when it passes every rule. These are applied daily, so a fund that drifts out of its mandate drops out on its own, and returns when it comes back:

| Rule | Passes when | Why |
|------|-------------|-----|
| Region | US or International | A Global fund has no single reference market to measure overweights against. |
| Size | Large or all-cap | The funds are built from large caps; small- and mid-cap managers' overweights are measured against a different opportunity set. |
| Number of stocks | 20 to 200 | Fewer than 20 is a concentrated fund where every holding is an overweight; above 200 the fund is close to an index and its tilts are small. |
| Top sector | ≤ 40% of the fund | A sector fund's overweights reflect its mandate, not stock selection. |
| Top country | ≤ 50% (a US fund's top country may be the US) | Single-country international funds (Japan, India, China) – same reason. |
| Emerging markets | International funds ≤ 40% emerging | "International" means developed international; the reference benchmark is mostly developed. |
| Holdings freshness | Latest holdings ≤ 14 days old | Ideas must be current. |
| Duplicate | Only one row per fund | A fund moved to a new trust is briefly listed twice under one ticker; it must count once. |

Anything unknown fails its rule. As of November 2026, 175 ETFs are active (138 US, 37 International) from 108 sponsors, out of about 800 equity funds profiled.

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

**Region** – a company is **US** when its primary listing is on NYSE / NASDAQ / AMEX, **International** otherwise. So TSMC, ASML, SAP and Shopify are International (home listings abroad) while Eaton, Medtronic, Linde and Arm are US. The same rule is used everywhere: fund profiles, benchmarks and fund region filters.

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

The market is what the Vanguard total-market funds hold, each company at its float-adjusted size: VTI for the US; VEA + VWO for International. Each week, the **breakpoint** at coverage *c* is the company size at which the largest companies reach *c* of the market's total float-adjusted value. As of September 2026:

| Coverage | US | International |
|----------|----|---------------|
| 70% | $94B (122 companies) | $15B (551) |
| 80% | $50B (226) | **$7.4B (983) – International benchmark** |
| 90% | $16B (473) | $3.1B (1,908) |
| 93% | **$10.1B (637) – US benchmark** | $2.2B (2,413) |

- **US large cap**: 93% coverage – the share of the US market the Russell 1000 represents.
- **International large cap**: 80% coverage – the developed + emerging large-cap segment in the MSCI / FTSE tradition.
- **Fund profiles** use the 70% / 90% lines to call a stock large / mid / small (Morningstar-style).

### The reference benchmarks

Four synthetic benchmarks are rebuilt weekly from the full screened universe (every listed company above the screening threshold, across ~40 exchanges):

| Benchmark | Members |
|-----------|---------|
| **US Large Cap Blend** | US companies with whole market cap ≥ the 93% breakpoint and float ≥ 10% |
| **US Large Cap Value** | The blend's value companies |
| **US Large Cap Growth** | The blend's growth companies |
| **International Large Cap Blend** | International companies with whole cap ≥ the 80% breakpoint and float ≥ 10% |

Membership follows the S&P / Russell convention – size measured on the whole company, with a minimum float. **Weights are proportional to the whole company's market cap** – the size the managers themselves see and size positions against ([§14](#14-design-decisions-and-known-limitations)). In the style benchmarks each company belongs wholly to one side and each side is re-weighted to 100%. There is no International style split: the style reference data is US-only, and about a third of the international benchmark has no reliable style.

Each ETF is linked to its region's blend benchmark.

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

**Three references.** The same holdings are measured three ways, and every fund chooses one:

| Reference | Reference weight | What it captures |
|-----------|------------------|------------------|
| **Full universe** (default) | The company's weight in the ETF's regional large-cap blend benchmark (0 if not a member) | Overweight versus the whole market – the conventional definition of active weight. |
| **Value / growth split** | US companies: the company's weight in the benchmark **of its own style** (value companies against US Large Cap Value, growth against US Large Cap Growth). International: as full universe. | Overweight versus the company's own style peers. A value manager holding a growth mega-cap at below its growth-index weight isn't credited with a conviction it doesn't have, without needing to classify the manager itself. |
| **Self** | The company's market cap ÷ the total market cap of the ETF's own holdings – the ETF re-weighted by market cap | Tilt within the manager's own chosen set: which of its stocks it holds more of than size alone would suggest. Independent of any benchmark, and of how the manager chose the names. |

**Selection.** Companies with a positive active weight are ranked from the largest overweight down. An active weight above **20%** is treated as abnormal (a data problem or an extreme concentrated bet) and dropped. A company of **another region than the ETF's** – a US fund's holding in TSMC, Shell or Brookfield – keeps its place in the weights but is never one of that ETF's best ideas: a fund only takes companies of its own region from ETFs of that region ([§10](#10-building-a-fund)), so such an idea could never be bought and would only push a usable one out of the ETF's top 10. Without this rule a US fund's foreign holding, which has no weight in the US benchmark, often ranked #1. The **top 10** per ETF are that manager's best ideas, with their rank (1 = the largest overweight).

**What the full-universe measure implies.** A stock outside the benchmark (below the large-cap line) has a reference weight of zero, so its whole ETF weight is active weight. Large-cap funds therefore apply the size test themselves ([§10](#10-building-a-fund)) so these never enter as new buys.

## 10. Building a fund

### The fund's mandate

Each fund is defined by a strategy:

| Parameter | Options |
|-----------|---------|
| Number of holdings | e.g. 30, 40, 50 |
| Reference for best ideas | Full universe, value / growth split, or self ([§9](#9-measuring-a-managers-best-ideas)) |
| Region | US, International, or a fixed split (e.g. 70% US / 30% International by number of holdings) |
| Size | Large (above its region's benchmark line with ≥ 10% float), mid / small (below it), or any |
| Style | Value, growth, any, or a fixed split (e.g. 60% growth / 40% value by number of holdings) |
| Rank depth | Which ranks of each manager's list count – e.g. only each manager's #1, or their top 3 |
| ESG | Optionally, ESG-qualified companies only ([§12](#12-esg-screen)) |
| Weighting | Equal, or market-cap based ([below](#weighting)) |
| Rebalancing frequency | Every week or every two weeks |
| Restrictions | Optionally specific ETFs only, or specific exchanges |

Every fund draws on **all active ETFs** unless restricted.

### The candidate list

1. **Collect** each active ETF's latest best ideas within the fund's look-back (its rebalancing period), in the fund's chosen reference.
2. **Filter** by the mandate. Region applies to both sides: a US fund takes US companies from US-region ETFs; an International fund, International companies from International ETFs. Size, style, exchange and ESG as configured.
3. **Combine across managers**, per company:
   - its **best rank** with any manager;
   - its **appearances** – how many managers rank it at that best rank;
   - its **largest active weight** among them, and the ETF that holds it (the *source ETF*).
4. **Order**: best rank first, then most appearances, then largest active weight. Every manager counts equally, whatever its size or the date of its holdings within the look-back.

So a stock that is the #1 overweight of four managers comes ahead of one that is the #1 of one manager, which comes ahead of anyone's #2.

5. **The ideal portfolio** is the top of this list, within the rank depth, up to the number of holdings. With a style split, the growth and value lists are built separately and each fills its share of the holdings. With a region split, the International list is built first and those companies are excluded from the US list, then each fills its share.

### Weighting

- **Equal weight**: 1 / number of holdings.
- **Market-cap based**: proportional to the **square root** of each company's market cap – a company 100× larger gets 10× the weight, not 100×, so the fund keeps a size tilt without being dominated by its largest names. Then each position is capped at **10%** and floored at **1%**, the difference redistributed across the others in proportion to their weights.

## 11. Maintaining a fund: rebalancing and turnover

Funds are re-evaluated on the weekly generation day, when their rebalancing period has passed. At each rebalance, starting from the current holdings:

| Current holding | Action |
|-----------------|--------|
| Still in the ideal portfolio | **Keep** |
| No longer among any manager's ideas within the fund's rank depth + a buffer (5 ranks for market-cap funds, 2 for equal-weight funds) | **Sell** – "not in best ideas" |
| Still a best idea, but its rank has worsened by 5 or more (market-cap funds) / 2 or more (equal-weight) since it was bought | **Sell** – "dropped in ranking" |
| Still a best idea, worsened by less | **Keep** – tolerated to limit turnover |

Free positions are then filled from the ideal portfolio in order (**buys**); with a region split, region by region – each region's free positions from its own ideas, up to its share, so a sold international holding is replaced by an international idea. Holdings are never sold just to restore the split, and a region short of ideas isn't made up from the other. A held stock is not replaced merely because a better-ranked idea has appeared: positions are only freed by sells. Weights are then recalculated for the whole portfolio.

Other maintenance rules:

- **A large-cap holding that slips below the large-cap line** is not sold for that reason alone – it stays eligible while managers keep ranking it – but such a stock can never be a new buy.
- **Corporate changes**: holdings are tracked at company level, so a change in a company's primary listing (e.g. after a re-listing) doesn't trigger a sale and re-purchase.
- **No ideas at all** (e.g. a data outage): the fund carries its holdings over unchanged rather than liquidating.

Every buy and sell is recorded with its reason, rank, number of managers and contributing ETFs.

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
- **Rank before breadth.** A company's best rank with any manager outweighs how many managers hold it; breadth breaks ties. This favours each manager's single strongest conviction over consensus names.
- **Every manager counts equally**, regardless of assets under management or holdings date – the method aggregates opinions, not money.
- **Style split trade-off.** Under the value / growth reference, a manager holding both styles is measured against two benchmarks in which large companies weigh more than in the blend (×2.3 for value, ×1.75 for growth), so its largest holdings rank somewhat lower than under the blend. Accepted, in exchange for not crediting out-of-style mega-caps as convictions.

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
| **Appearances** | Number of managers ranking a company at its best rank. |
| **Best idea** | One of the top 10 positive active weights of an ETF in a week. |
| **Breakpoint** | The company size at which the market's largest companies reach a given share of total market value. |
| **Company market cap** | The whole company's equity value, all share classes, in USD. |
| **Float factor** | The investable share of a company's equity, from the index funds' holdings or free float. |
| **Primary listing** | The listing that represents a company: its home-market ordinary shares. |
| **Rank** | A company's position in a manager's best-ideas list (1 = largest overweight). |
| **Rank depth** | How many of each manager's ranks a fund draws on. |
| **Source ETF** | The ETF with the largest active weight in a fund holding. |

---

## Appendix A – Parameter reference

| Parameter | Value |
|-----------|-------|
| Active ETF: SEC list window | Last filing within 15 months |
| Equity fund: minimum stock share | 75% |
| Fund region: US / International | ≥ 80% / ≤ 20% US |
| Fund style: value / growth | ≥ 60% of style-classified weight |
| Selection: stocks | 20–200 |
| Selection: top sector / top country | ≤ 40% / ≤ 50% |
| Selection: International emerging share | ≤ 40% |
| Holdings: maximum age | 14 days |
| Market cap: validation window | 21 days; excluded after 5 days without a good value |
| Large cap: US / International coverage | 93% / 80% of the market |
| Large cap: minimum float | 10% |
| Fund profile size lines | 70% (large) / 90% (small) |
| Best ideas: minimum priced holdings | 90% |
| Best ideas: market cap window | ± 5 days of the holdings date |
| Best ideas: maximum active weight | 20% |
| Best ideas: per ETF | 10 |
| Fund: rank buffer / sell on rank drop | 5 (market-cap funds), 2 (equal-weight funds) |
| Fund weighting: exponent / cap / floor | 0.5 (square root) / 10% / 1% |
| ESG: risk rating / ESG score / governance | AAA–B / ≥ 50 / ≥ 50 |
| Weekly generation | Wednesday on Tuesday's close (moved around NYSE holidays) |
