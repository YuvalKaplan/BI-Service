# Best Ideas – Investment Methodology

How the model portfolios are populated and maintained: which managers we listen to, where the data comes from, how a manager's "best ideas" are measured, and how those ideas become a portfolio. It is written for investment professionals; the software side of the same process is in [README.md](../README.md).

Figures quoted "as of" a date are illustrations of the rules at that date, not parameters – the parameters are listed in [Appendix A](#appendix-a--parameter-reference).

---

## Contents

1. [The idea and the process](#1-the-idea-and-the-process)
2. [Data sources](#2-data-sources)
3. [The manager universe: actively managed equity ETFs](#3-the-manager-universe-actively-managed-equity-etfs)
4. [Holdings](#4-holdings)
5. [Company data and data quality](#5-company-data-and-data-quality)
6. [Market size and the reference benchmarks](#6-market-size-and-the-reference-benchmarks)
7. [Each company's peers: style and market](#7-each-companys-peers-style-and-market)
8. [Measuring a manager's best ideas](#8-measuring-a-managers-best-ideas)
9. [Building a fund](#9-building-a-fund)
10. [Maintaining the model portfolio](#10-maintaining-the-model-portfolio)
11. [ESG screen](#11-esg-screen)
12. [Audit trail](#12-audit-trail)
13. [Design decisions and known limitations](#13-design-decisions-and-known-limitations)
14. [Glossary](#14-glossary)
- [Appendix A – Parameter reference](#appendix-a--parameter-reference)

---

## 1. The idea and the process

An active manager's portfolio mixes two things: positions held because the manager has a view, and positions held to stay close to a benchmark, control risk or stay diversified. The positions the manager holds **well above their market weight** are the ones they are most willing to be wrong about – their highest-conviction calls, their *best ideas*. Academic work on mutual funds (Cohen, Polk and Silli, *Best Ideas*, updated 2021) found that managers' largest overweights outperformed, while the rest of the portfolio largely did not.

The model portfolios follow the managers of US-listed, actively managed equity ETFs with a broad, diversified mandate. Each manager's overweights are its candidate ideas, and each is measured against the company's own peers in the market. A model portfolio holds the companies that the most managers rank most highly, within the rules of its fund's mandate.

Active ETFs are used rather than mutual funds because they publish their holdings far more often (most daily; we read them weekly – [§4](#4-holdings)), so the ideas are recent rather than a quarter old.

### The process

| Step | What happens | Cadence |
|------|--------------|---------|
| Manager universe | Every ETF that tells the SEC it is not an index fund; its profile decides whether it is a diversified equity fund that qualifies ([§3](#3-the-manager-universe-actively-managed-equity-etfs)) | Monthly list, rules re-checked daily |
| Holdings | Each qualifying ETF's holdings, every line identified as a company ([§4](#4-holdings)) | Daily (Tue–Sat); most ETFs refresh weekly |
| Company data | Prices, market caps, share-class consolidation, region, validation ([§5](#5-company-data-and-data-quality)); each company's style and each International company's market ([§7](#7-each-companys-peers-style-and-market)) | Daily |
| Size breakpoints | Where "large cap" ends, measured from the total market ([§6](#6-market-size-and-the-reference-benchmarks)) | Weekly |
| Reference benchmarks | US and International large blends; the US blend's value and growth halves, the International blend's developed and emerging halves ([§6](#6-market-size-and-the-reference-benchmarks)) | Weekly |
| Best ideas | Top 10 active weights per ETF – a Global ETF's per region part – against two references – the market and the ETF itself ([§8](#8-measuring-a-managers-best-ideas)) | Weekly |
| Funds | Candidate list across all managers → portfolio → buys / sells / weights ([§9](#9-building-a-fund), [§10](#10-maintaining-the-model-portfolio)) | Weekly or every two weeks, per fund |

**Weekly timing.** The weekly generation runs on Wednesday on **Tuesday's closing data**: benchmarks, best ideas and funds all use the same close. When Tuesday is an NYSE holiday, the generation moves a day later (Thursday, on Wednesday's close), else a day earlier, staying in the same week. A fund's recalculation frequency is counted in calendar weeks, so a moved generation never makes a weekly fund skip a week.

## 2. Data sources

| Source | Used for |
|--------|----------|
| **SEC EDGAR – Form N-CEN** | Which ETFs are actively managed: every registered fund's annual report of its own type ([§3](#3-the-manager-universe-actively-managed-equity-etfs)). Read directly from EDGAR, the day after filing. |
| **SEC EDGAR – fund prospectuses** | Which ETFs are non-transparent: the legend the SEC requires on their prospectus cover, found by EDGAR's full-text search ([§3](#3-the-manager-universe-actively-managed-equity-etfs)). Checked monthly. |
| **Financial Modeling Prep (FMP)** | ETF holdings and profiles (asset class, sector weights, sponsor, AUM); company profiles (identifiers – ISIN, CUSIP, SEC CIK – name, domicile, sector, currency, trading status); daily prices and market caps with history; shares outstanding and free float; quarterly financials (share counts); fundamental factors (growth, margins, valuation ratios, yields) for style classification; ESG scores and risk ratings; FX rates; the stock screener (all large companies by exchange); exchange holiday calendars. |
| **Vanguard index funds – VTI, VEA, VWO** (holdings via FMP) | The "whole market": CRSP US Total Market (VTI), FTSE Developed All Cap ex US (VEA) and FTSE Emerging Markets All Cap China A Inclusion (VWO). They hold nearly every listed company at its float-adjusted weight, so they define the size breakpoints and float factors ([§6](#6-market-size-and-the-reference-benchmarks)). |
| **Style reference ETFs** – Vanguard VUG / VTV / VBK / VBR, iShares Morningstar ILCG / ILCV / IMCG / IMCV / ISCG / ISCV (holdings via FMP) | Which companies the index providers (CRSP, Morningstar) classify as value or growth ([§7](#7-each-companys-peers-style-and-market)). |

All market data comes from APIs; nothing is scraped. Every figure is converted to **USD at that date's exchange rate**, from the currency the listing actually trades in.

## 3. The manager universe: actively managed equity ETFs

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
| **Region** | Share of the fund in US companies (by each company's region – [§5](#5-company-data-and-data-quality)). ≥ 80% US → US; ≤ 20% → International; otherwise Global. |
| **Size** | Each stock large above its market's 70% breakpoint, small below the 90% one, mid between (Morningstar's approach). The fund is *large* with ≥ 70% large stocks, *small* with ≥ 50% small, *mid* with ≥ 50% mid, *smid* with ≥ 70% mid + small, otherwise *all-cap*. |
| **Style** (large funds) | Share of value vs growth companies: ≥ 60% one way, else blend. |
| **Stock count** | Lines matching a stock the total-market index funds hold – directly, or through the company: a depositary receipt (ADR) counts as its company's stock, which the index funds hold in its home market. |
| **Sector concentration** | FMP's sector weights (else our companies' sectors): the largest sector – for information; the selection's sector rule is measured on the stored holdings (below). |
| **Country concentration** | Our companies' country of domicile: the largest country. |
| **Emerging-market share** | Share held in stocks VWO holds (FTSE's classification: Taiwan, China, India, Brazil emerging; Korea developed) – for information. |

### Selection rules

A profiled equity fund feeds the best ideas (**active**) when it passes every rule. The rules keep out funds whose overweights would reflect a mandate rather than stock selection within a broad large-cap market: **derivative strategies, funds that follow an index, equal-weighted funds, funds that don't publish their holdings daily, sector and thematic funds, single-country funds, and funds investing mostly outside the large-cap universe the reference benchmarks measure** ([§6](#6-market-size-and-the-reference-benchmarks)). The rules are applied every time holdings are read, so a fund that drifts out of its mandate drops out on its own and returns when it comes back – except a fund that has used derivatives, which never returns.

| Rule | Passes when | Why |
|------|-------------|-----|
| Index name | The fund's name carries no index provider's brand (Dow Jones, S&P, MSCI, FTSE, Russell, STOXX, Nasdaq and the like) nor the word "index" – the fund company's own name aside | A fund named after an index follows it, even when it reports to the SEC as actively managed – a Shariah fund mirroring the Dow Jones Islamic Market International Titans 100, for instance. Its overweights are the index's screen, not a manager's choices. A fund company named like an index provider (Russell Investments) is a manager, so its own name doesn't count. |
| Holdings transparency | The fund publishes its holdings daily: no prospectus of its own filed in the last 15 months carries the SEC's non-transparent ETF legend ("This ETF is different from traditional ETFs…") | A non-transparent fund (ActiveShares, proxy portfolio, tracking basket) discloses its actual holdings only quarterly, a month or two late, and publishes a substitute basket in between. The data vendor dates that quarterly file afresh with every refresh, so it passes the freshness rule while being months old: its "best ideas" would be last quarter's positions. |
| Region | US, International or Global | A Global fund (between 20% and 80% US) has no single reference market, so it is measured in two parts – its US companies and its International ones – each as a fund of that region ([§8](#8-measuring-a-managers-best-ideas)). |
| Size | Large or all-cap | The funds are built from large caps; small- and mid-cap managers' overweights are measured against a different opportunity set. |
| Number of stocks | 20 to 200 (US), 20 to 250 (International); a Global fund **20 to 200 US and 20 to 250 International companies** – each part as many as a fund of its region | Fewer than 20 is a concentrated fund where every holding is an overweight; above the maximum the fund is close to an index and its tilts are small. International managers spread over more markets and names, so their limit is higher. A Global fund's part of a few stocks – a US-heavy fund's handful of foreign names – would make nearly all of them best ideas, so a fund short in either region is excluded. |
| Derivatives | The fund's holdings have **never** shown an option contract or an equity-linked note | Covered-call, option-income and hedged funds hold an ordinary-looking stock portfolio, but their weights follow the option book, not the manager's view of the stocks. Once seen, the fund is excluded permanently: such funds write options intermittently, and a week without contracts doesn't change the strategy. |
| Sector concentration | No sector more than **20 points** above its reference weight | A sector's reference weight is the highest weight it has in any of the most specific reference benchmarks – US large value, US large growth, International large developed, International large emerging. A sector or thematic fund's overweights reflect its mandate, not stock selection. |
| Top country | ≤ 50% (a US fund's top country may be the US); a Global fund's largest country outside the US ≤ 50% of its International part | Single-country international funds (Japan, India, China, Korea – 80–100% in one country) – same reason. Half rather than a third: an emerging-markets fund holds 25–45% in Taiwan or China, as the emerging benchmark does. A Global fund's US part is like a US fund; its International part must be diversified like an International fund. |
| Benchmark coverage | ≥ 70% of the fund's stock weight in companies of its region's large-cap blend benchmark to enter; ≥ 65% to stay; a Global fund each part against its own region's, the lower of the two counting | A fund mostly outside the benchmark – mid and small caps, frontier markets, private holdings – has overweights the benchmark cannot measure: every holding outside it is an "overweight" by default. |
| Equal weighting | Not an equal-weighted fund: the fund says it weights its holdings equally (in its name or its description) **and** its holdings are flat – its 90th-percentile position under **2×** its 10th-percentile one | An equal-weighted fund's weights come from its method, not from the manager's view of each stock: every overweight is the same size less the company's benchmark weight, so its "best ideas" would simply be its smallest companies, ordered by how prices have drifted since the last rebalance. |
| Holdings freshness | Latest holdings ≤ 14 days old | Ideas must be current. |
| Duplicate | Only one row per fund | A fund moved to a new trust is briefly listed twice under one ticker; it must count once. |

Anything unknown fails its rule – except the rules measured on the fund's stored holdings (sector concentration, benchmark coverage, a Global fund's stocks per part, equal weighting), which a newly listed fund passes until its first holdings are read; it does not become active before then.

### How the selection rules are measured and set

The rules aim to remove the fund types that don't express stock selection within a broad market, while removing as few ordinary diversified funds as possible. The principles behind them:

- **Behaviour, not names.** Funds are excluded for what they hold, never for what they are called: a fund name is a marketing choice, and the same words ("innovation", "leaders", "quality") are used by thematic and diversified funds alike. The only name rules are in the equity classification above (leveraged, buffer and option-income funds) and the index-name rule, where the name states a structure rather than a theme: a fund that names an index provider's index has licensed it and follows it. The classification's rules are backed by the derivatives rule, which reads the holdings; an index follower's holdings look like any diversified manager's, so for it the name is the only evidence. The data vendor's fund descriptions aren't used for the index rule: they are paraphrases, and describe funds that say they follow no index as "mirroring" one.
- **Equal weighting by statement and holdings together.** Neither is enough alone. A fund's own words – its name or description – say whether equal weighting is its method, but the data vendor's description paraphrases (one concentrated manager's says it mirrors an equal-weight world index), and some funds that describe equal weighting apply it to part of the portfolio or drift far from it. Flat holdings alone don't show the method either: many concentrated managers size 20–40 picks about the same by choice, and a flatness test alone would remove some 8% of the active funds, most of them such managers. Flatness is measured as the 90th-percentile position over the 10th-percentile one – 1 for equal weights – which ignores the few largest and smallest positions (a holding that has run up, a stake being sold); equal-weighted funds measure 1.3–1.7 between rebalances, the funds that describe equal weighting without applying it 5 or more, and the median active fund about 4.75.
- **Transparency by the prospectus, not the annual filing.** A non-transparent ETF operates under an SEC order that requires a legend on its prospectus cover ("This ETF is different from traditional ETFs. Traditional ETFs tell the public each day what securities they own…"); its own single-fund prospectus is the evidence, and a fund that converts to full transparency drops the legend. The fund's annual filing has a related question – whether it relied on the SEC's ETF rule, which requires daily holdings disclosure – but it is answered unreliably: in November 2026 one sponsor's four non-transparent funds said they relied on it, and 27 fully transparent active funds left it blank. The holdings data alone can't tell either: a non-transparent fund's file stays unchanged for weeks and then changes all at once, but so can a fully transparent fund's with few flows.
- **Derivatives by the holding lines.** A holding line is an option when it carries an expiry date with a strike and a call / put marker – in any of the formats the data vendor uses ("AAPL US 10/16/26 C340", "SPY 11/17/2026 762.29 C", "SPY CALL USD 12/04/2026", "JPMORGAN CHASE + CO NOV26 355 CALL", the exchanges' standard option code "USAR 261016C00020000") – and an equity-linked note when it is named as one ("WELLS FARGO ELN 10/27") or shows a participation level of 20% or more before a maturity date ("Wells Fargo & Co 147.40% 12/15/2026"). Treasury bills, money-market sweeps, currency forwards and rights also carry dates but never a strike, a call / put marker or a note level, so they don't trigger the rule. Every holding set read since a fund entered the universe counts, as does the holding set read for its monthly profile.
- **Sector concentration against the market, by style and region.** A fixed ceiling on the top sector (the earlier rule: 50%) penalises a growth manager for holding what the growth market holds – technology is over half of the US large growth benchmark – while letting through a fund concentrated in a small sector. Measuring each sector against its own reference weight treats every sector alike: a growth manager may hold as much technology as the growth benchmark, a value manager as much finance as the value benchmark, plus the margin, whereas a defence, retail or semiconductor theme exceeds every benchmark's weight in its sector. The reference is the highest weight across the region / style benchmarks rather than the fund's own region or style, so a fund is never penalised for its style label (a "blend" fund tilted to growth is measured as generously as a growth fund). Sector weights are taken from the fund's identified stock holdings, using our companies' sectors – the same classification as the benchmarks.
- **Benchmark coverage as the universe test.** Best ideas are overweights against the region's large-cap benchmarks (in the US, the blend's value and growth halves); a holding outside the blend has no benchmark weight, so it is an overweight by definition. The share of the fund's identified stock weight in companies of that benchmark (each company counted once, across share classes) measures how far the manager invests in the universe the benchmark describes. Thematic and concentrated funds tend to fail it as well, because their themes run into mid and small caps.
- **A band, not a line, for a measure that moves weekly.** Coverage changes with prices, trades and the benchmark's weekly reconstitution, so a fund near a single line would drop in and out. A fund enters at 70% and stays while at 65% or more; the stay level applies only to a fund that entered under the entry level, so every fund first faces 70%.
- **Round levels.** The levels are set in steps of 5 points rather than at the exact point that separates today's funds: a level fitted precisely to one week's pool is fitted to noise and stops working as funds enter and leave. Each level was chosen where it removes the funds of the targeted type – single-country, sector-concentrated, out-of-universe – with the fewest ordinary diversified funds removed, and was checked against the funds on either side of it.
- **Emerging markets measured, not excluded.** Until November 2026 International funds could hold at most 35% in emerging markets: measured against the whole international market, a dedicated emerging-markets manager's largest overweights were simply the largest emerging-market companies – an allocation between developed and emerging markets, not stock selection – and that rule left out about 60 International funds. Since the International benchmark was split into developed and emerging halves ([§6](#6-market-size-and-the-reference-benchmarks)), each company is measured against its own market's benchmark, so an emerging-markets manager's overweights are again its stock picks, and the rule was dropped. The top-country limit was raised from 35% to 50% at the same time: a diversified emerging-markets fund holds 25–45% in Taiwan or China, as its benchmark does, while a single-country fund holds 80–100%.
- **Accepted residue.** Some thematic funds are diversified enough to pass every test – their holdings look like a broad large-cap portfolio with tilts. They are accepted: by the behavioural tests they are indistinguishable from an ordinary manager with a view, and excluding them would take a name rule.

## 4. Holdings

**Frequency and dating.** Holdings are read from FMP every weekday run. FMP refreshes most funds once a week (dated Sunday), some on other days; each holding set is stored under FMP's own date. A fund's holdings are used for up to 14 days.

**Identifying each line.** Every holding line is matched to a company:

- by its symbol, but only when the security behind that symbol agrees with the line's ISIN / CUSIP or name (a holding's symbol can belong to a different security elsewhere);
- else by ISIN;
- else by name, accepting a candidate only when every meaningful word of the shorter name matches the longer one – never on a single shared word.

Lines that aren't stocks are recognised and excluded from the calculation: cash and sweeps, currencies and FX legs, money-market and other funds, derivatives and options, bonds, equity-linked notes (which often carry their issuer's symbol – an ELN on JPMorgan is not JPMorgan stock), rights, warrants and private holdings.

**Duplicate lines.** When a fund lists the same company on several lines (separate lots), they are summed when they imply the same share price; when the prices disagree they are most likely different securities mis-identified as one, and that company is **quarantined** – left out for that fund that week – rather than risk a wrong weight.

## 5. Company data and data quality

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

FMP's free float and the total-market index funds' holdings give each company a **float factor** (the investable share of its equity). It is not used to weight anything ([§13](#13-design-decisions-and-known-limitations)), but a company needs **at least 10% float** to count as large cap – S&P's minimum. It also identifies exchange-traded notes and preferreds named like their issuer (zero equity float), which are never treated as the company.

## 6. Market size and the reference benchmarks

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

Six synthetic benchmarks are rebuilt weekly from the full screened universe (every listed company above the screening threshold, across ~40 exchanges):

| Benchmark | Members |
|-----------|---------|
| **US Large Cap Blend** | US companies with whole market cap ≥ the 93% breakpoint and float ≥ 10% |
| **US Large Cap Value** | The blend's value companies |
| **US Large Cap Growth** | The blend's growth companies |
| **International Large Cap Blend** | International companies with whole cap ≥ the 80% breakpoint and float ≥ 10% |
| **International Large Cap Developed** | The International blend's developed-market companies |
| **International Large Cap Emerging** | The International blend's emerging-market companies |

Membership follows the S&P / Russell convention – size measured on the whole company, with a minimum float. **Weights are proportional to the whole company's market cap** – the size the managers themselves see and size positions against ([§13](#13-design-decisions-and-known-limitations)). In the style and market halves each company belongs wholly to one side and each side is re-weighted to 100%. The International blend splits by market, not by style ([§7](#7-each-companys-peers-style-and-market)). Emerging markets are about 35% of the International blend by whole market cap (26.5% float-adjusted, November 2026).

Each ETF is linked to its region's blend benchmark – through it, to the blend's halves its holdings are measured against: value and growth in the US, developed and emerging internationally ([§8](#8-measuring-a-managers-best-ideas)).

## 7. Each company's peers: style and market

A company's active weight is measured against its peers in the market ([§8](#8-measuring-a-managers-best-ideas)). For a US company these are the US companies of its style, and for an International company the International companies of its market. The same labels decide which part of a split fund the company competes for ([§9](#9-building-a-fund)).

US companies are grouped by style and International companies by market. The index providers' style classifications cover US companies only, and about a third of the International benchmark has no reliable style. Every International company's market, by contrast, is known from the index provider.

### Value or growth

Each company carries one style, **value** or **growth**, used by the US style benchmarks and by style-restricted funds, from the first of these sources that classifies it:

1. **Index provider classification** – companies held by the style reference ETFs (CRSP via Vanguard, Morningstar via iShares, large / mid / small). Each reference ETF holding a company is one vote for its style; the majority wins, and a tie keeps the existing style (the two families overlap). These are re-read weekly and re-applied daily, so styles follow the index providers' reconstitutions.
2. **Active managers' classification** – a company not in the reference ETFs but held by active ETFs whose own profile is value or growth takes that style.
3. **Statistical classifier** – for the rest (most international companies), a gradient-boosting model trained on the reference-classified companies' fundamentals (size, sector, industry, growth rates, margins, P/E, P/B, yields) predicts value or growth.

### Developed or emerging

Each International company is **developed** or **emerging** in FTSE's classification, the index family behind the international index funds. Under it, Korea and Poland are developed, while Taiwan, China, India, Brazil and Saudi Arabia are emerging. The market comes from the first of these that decides:

1. **The index provider's assignment** – a company held by the emerging-markets index fund (FTSE Emerging) is emerging, one held by the developed ex-US fund (FTSE Developed) developed; held by both, the one holding it at least twice the other's float-adjusted value decides.
2. **Its country** – a company neither fund holds takes the classification of its country, as the index-held companies of that country have it (so a reclassification by the index provider follows on its own). A tax haven or a US country of record (a depositary receipt's or a US holding company's) says nothing about the market.
3. **Its country of incorporation**, the same way.

US companies count as developed. A company none of these decide, such as one in a frontier market like Kazakhstan or Argentina, belongs to neither market.

## 8. Measuring a manager's best ideas

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
| **Market** (default) | US companies: the company's weight in the benchmark **of its own style** – value companies against US Large Cap Value, growth companies against US Large Cap Growth (the few with no style against the US blend). International companies: their weight in the benchmark **of their own market** – developed companies against International Large Cap Developed, emerging ones against International Large Cap Emerging (the few of neither against the International blend). 0 if not a member. | Overweight versus the company's own peers in the market. A value manager holding a growth mega-cap at below its growth-index weight isn't credited with a conviction it doesn't have, as it would be against the whole US market; an emerging-markets manager's holding of TSMC is measured against TSMC's weight among emerging-market companies, not against the whole international market, where every large emerging-market company would look like an overweight. The manager itself needs no style or market label: each holding is compared with its own peers' benchmark, and all of the ETF's active weights are ranked together. |
| **Self** | The company's market cap ÷ the total market cap of the ETF's own holdings – the ETF re-weighted by market cap | Tilt within the manager's own chosen set: which of its stocks it holds more of than size alone would suggest. Independent of any benchmark, and of how the manager chose the names. |

The reference is decided by the **company's** region, style and market, never by the fund's: a fund's region and style settings only decide what it may buy ([§9](#9-building-a-fund)). Measuring against the whole US large-cap market (the blend) was the earlier default; it was dropped in November 2026 in favour of the style benchmarks, the more meaningful measure of a manager's conviction.

**Selection.** Companies with a positive active weight are ranked from the largest overweight down. An active weight above **20%** is treated as abnormal (a data problem or an extreme concentrated bet) and dropped. A company of **another region than the ETF's** – a US fund's holding in TSMC, Shell or Brookfield – keeps its place in the weights but is never one of that ETF's best ideas: a fund only takes companies of its own region from ETFs of that region ([§9](#9-building-a-fund)), so such an idea could never be bought and would only push a usable one out of the ETF's top 10. Without this rule a US fund's foreign holding, which has no weight in the US benchmark, often ranked #1. The **top 10** per ETF are that manager's best ideas, with their rank (1 = the largest overweight).

**Global managers: two parts.** A Global fund is measured as two funds: its **US companies** and its **International companies**, each re-weighted to 100% of that part – the manager's weight in the company within the region – and measured against that region's reference exactly as a US or an International fund's holdings are, with its own top 10. A Global fund 60% in the US holding 3% in Microsoft has 5% of its US part in it; 2% in Nestlé is 5% of its 40% International part. A company of no known region is in neither part. Measuring the whole fund against one combined world benchmark (the two regional benchmarks at their market shares) would skew its ideas three ways:

- **Region allocation would leak into every stock.** A Global manager underweight the US by five points shows every US holding a little less overweight, and every International one a little more – a call on regions, not on stocks. Within each part the region's weight is the manager's own, so only stock selection is measured.
- **The style benchmarks would favour International companies.** US companies are measured against their own style's benchmark, where a large company weighs 1.75× (growth) to 2.3× (value) its weight in the whole US market; International companies against their own market's (developed or emerging). Ranked together, US holdings would show smaller overweights and the International ones would crowd the top 10. Ranked within each part, a US company competes only with US companies measured the same way.
- **Ranks would not compare.** One top 10 across both regions spreads a Global manager's ideas over the fund's two regional lists at lower ranks (2, 5, 9 rather than 1, 2, 3), so in the consensus score ([§9](#9-building-a-fund)) a Global manager would count for less than a regional one. Each part's own top 10 scores like a regional manager's list, in its own region only.

**What the market measure implies.** A stock outside the benchmark (below the large-cap line) has a reference weight of zero, so its whole ETF weight is active weight. Large-cap funds therefore apply the size test themselves ([§9](#9-building-a-fund)) so these never enter as new buys.

## 9. Building a fund

### The fund's mandate

Each fund is defined by a strategy:

| Parameter | Options |
|-----------|---------|
| Number of holdings | e.g. 30, 40, 50 |
| Reference for best ideas | Market or self ([§8](#8-measuring-a-managers-best-ideas)) |
| Region | US, International, or Global: split by number of holdings – the **market split** (below) or a fixed one (e.g. 70% US / 30% International); the International holdings optionally split again into developed and emerging – the market split or a fixed one (e.g. 75% / 25%) |
| Size | Large (above its region's benchmark line with ≥ 10% float), mid / small (below it), or any |
| Style | Value, growth, any, or a split by number of holdings – the **market split** (US only) or a fixed one (e.g. 60% growth / 40% value) |
| Rank points | The points a company earns for each rank on a manager's list (by default 512 for #1 down to 1 for #10, each rank half the one above – [below](#the-candidate-list)) |
| ESG | Optionally, ESG-qualified companies only ([§11](#11-esg-screen)) |
| Weighting | Equal, or market-cap based ([below](#weighting)) |
| Recalculation frequency | Every week or every two weeks; staggered optionally ([§10](#10-maintaining-the-model-portfolio)). Every recalculation sets the holdings' target weights |
| Restrictions | Optionally specific ETFs only, or specific exchanges |

Every fund draws on **all active ETFs** unless restricted.

**The market split.** A fund can divide its holdings by the market's own weights instead of fixed numbers. There are three splits:

| Split | Divides | Measured on |
|-------|---------|-------------|
| Region | The holdings between the US and International | The US large-cap benchmark against the International large-cap benchmark ([§6](#6-market-size-and-the-reference-benchmarks)) |
| US style | The US holdings between growth and value | The US Large Cap Growth benchmark against the US Large Cap Value benchmark |
| International market | The International holdings between developed and emerging markets | The International Large Cap Developed benchmark against the International Large Cap Emerging benchmark |

Each split is measured on **the same benchmarks the best ideas are measured against**, so a fund is consistent from the measure of a manager's conviction to its own allocation. The managers decide which companies a fund holds, and the market decides how many of each kind. International has no style split and the US no market split.

The totals are **float-adjusted**, meaning each company's market cap is multiplied by the share of its stock that floats, as the index providers weight their indexes. On whole market caps, the state, family and cross holdings common outside the US, and especially in emerging markets, would count in full. In November 2026 the US share of the two regions was 58% on whole market caps against 65% float-adjusted, and the emerging share of International was about 35% against 26.5%.

The splits are recalculated **quarterly**, on the first weekly run of each calendar quarter, and each recalculation uses the latest one. Each share is applied to a number of holdings and rounded to the nearest whole number (a half rounds up), with the other side taking the rest: the US share to all the holdings first, then the growth share to the US holdings and the emerging share to the International ones. As of November 2026, for a 50-holding fund using all three splits:

| Split | Part | Weight | Holdings |
|-------|------|--------|----------|
| Region | US | 65.0% | 33 of 50 |
| | International | 35.0% | 17 of 50 |
| US style | Growth | 57.5% | 19 of 33 |
| | Value | 42.5% | 14 of 33 |
| International market | Developed | 73.5% | 13 of 17 |
| | Emerging | 26.5% | 4 of 17 |

### The candidate list

1. **Collect** each active ETF's latest best ideas within the fund's look-back (its recalculation period), in the fund's chosen reference.
2. **Filter** by the mandate. Region applies to both sides: a US fund takes US companies from US-region ETFs; an International fund, International companies from International ETFs. A Global ETF's US part counts as a US ETF and its International part as an International ETF, so a Global manager scores once in each region, with that part's list. Size, style, exchange and ESG as configured.
3. **Score each company across managers.** Each manager gives a company points for its rank on that manager's best-ideas list, and the points are summed into the company's **consensus score**:

   | Rank | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 |
   |------|---|---|---|---|---|---|---|---|---|----|
   | Points | 512 | 256 | 128 | 64 | 32 | 16 | 8 | 4 | 2 | 1 |

   Each rank is worth half the one above, so one manager's #1 (512) equals two managers' #2 or eight managers' #4. A manager counts once per company. Alongside the score, each company carries its **best rank** with any manager, its **appearances** (how many managers rank it) and its **largest active weight** among them, with the ETF that holds it (the *source ETF*).
4. **Order**: highest score first, then most appearances, then largest active weight. Every manager counts equally, whatever its size or the date of its holdings within the look-back.

So a stock that is the #2 of three managers (768) comes ahead of one that is the #1 of a single manager (512), and a stock ranked #4 to #10 by many managers can outscore one ranked #3 by only one. The ranking deltas within a manager's list, which can be marginal, matter less than how many managers agree.

5. **The ideal portfolio** is built in **buckets**, each ordered on its own: one per region of a region split, one per market (developed / emerging) of an International market split, and one per style of a style split. Each bucket's ideal holdings are the top of its list, up to its share of the holdings: on the November 2026 market split, 33 US and 17 International for a 50-holding fund with a region split alone, and 19 US growth, 14 US value, 13 International developed and 4 International emerging with all three splits. With a region split, the International list is built first and those companies are excluded from the US list. With a style split, a company of neither style (rare) is in no bucket; with a market split, likewise a company of neither market (frontier markets). An emerging bucket short of ideas leaves its places to the developed one.

### Weighting

- **Equal weight**: 1 / number of holdings.
- **Market-cap based**: proportional to the **square root** of each company's market cap – a company 100× larger gets 10× the weight, not 100×, so the fund keeps a size tilt without being dominated by its largest names. Then each position is capped at **10%** and floored at **1%**, the difference redistributed across the others in proportion to their weights.

## 10. Maintaining the model portfolio

This section describes the **model portfolio**: the list of holdings and their target weights that the method produces for each fund. Its buys and sells are changes to that list, not trades. A fund that follows the model trades to match it, and keeps its positions close to their target weights as prices move, by its rebalancing rules (end of this section).

A model portfolio is recalculated on the weekly generation day once its recalculation period – one or two weeks – has passed. A staggered model portfolio is recalculated every week and reviews half of its holdings each time ([below](#staggered-recalculation)).

### The recalculation

Each recalculation, in this order:

1. **Sell holdings that are no longer a best idea.** A holding that is no longer among the best ideas of any of its region's managers is sold.
2. **Sell holdings below their bucket's keep line.** A holding is kept while it is within the top *keep buffer* × its bucket's share of the holdings, by score. The keep buffer is a fund setting, 1.25 by default: a 50-holding model portfolio of 33 US and 17 International holdings keeps a US holding while it is within the top 42 US ideas, and an International one within the top 22. At 1.0 the keep line is the bucket's cut-off itself.
3. **Buy.** Each bucket fills its free places, up to its share, with its highest-scoring ideas not yet held, so a sold International holding is replaced by an International idea and the split stays exact. A bucket short of eligible ideas holds fewer than its share, and the shortfall is not filled from another bucket, except that an emerging bucket's empty places go to the developed one.
4. **Set the target weights.** Every holding is given its target weight under the fund's weighting scheme, equal or market-cap based ([§9](#weighting)).

With a buffer above 1.0, a holding is not replaced merely because a higher-scoring idea has appeared. Places are freed only by sells, so a new idea scoring inside the cut-off can wait outside the model portfolio until a place frees up. With a buffer of 1.0, the model portfolio holds exactly each bucket's top list, and an idea that rises into it replaces the bucket's lowest holding at the next recalculation.

### Staggered recalculation

Staggering is a fund setting. The holdings are divided into two halves: at the start each bucket's holdings are split by alternating down its ranking (ranks 1, 3, 5 … in one half), and a new holding joins the half of the holding whose place it takes. The model portfolio is recalculated twice per recalculation period, each half in turn; for a two-week period, that is every week, so each holding is reviewed every two weeks.

Step 1 applies to both halves: a holding that is no longer a best idea is sold whichever half it is in. Step 2 applies only to the half under review. A holding of the other half that has fallen below the keep line is kept until its own half's turn, and its bucket buys one company fewer that time. New ideas can therefore enter every week while each holding gets the full period, and the changes are spread over every week.

### Choosing the buffer and the schedule

A wider buffer means fewer changes to the list, and fewer trades for a fund that follows it, but the holdings it keeps are the ones whose managers' consensus has weakened. A buffer of 1.0 keeps a model portfolio on its current consensus, at the cost of higher turnover. A staggered schedule limits that turnover: a holding that slips below the line gets until its own half's turn to recover, while new ideas can still enter every week. Both choices are reviewed as history accumulates.

### When the split or a company's classification changes

When the quarterly market split changes, each bucket's share changes with it. With a buffer of 1.0, a bucket now above its share sells its lowest-scored holdings beyond it (in a staggered model portfolio, as each half comes up for review), and the freed places go to the bucket below its share, so the new split is reached within one recalculation period. With a buffer above 1.0, nothing is sold just to move it: the bucket above its share makes no buys until its own sells bring it down.

When a company's style or market changes, it moves to the other bucket, where it is measured against that bucket's benchmark and kept only while it is within that bucket's keep line.

### Other rules

- **A large-cap holding that falls below the large-cap line** is not sold for that reason alone. It stays while it keeps its place by score, but a company below the line can never be a new buy.
- **Corporate changes** don't change the list. Holdings are tracked by company, so a change in a company's primary listing, for example after a re-listing, doesn't cause a sale and re-purchase.
- **No ideas at all**, for example during a data outage: the model portfolio keeps its holdings unchanged rather than selling them.

Every buy and sell is recorded with its reason, score, best rank, number of managers and contributing ETFs.

### Rebalancing

The model portfolio sets target weights; it doesn't track how positions drift with prices. Rebalancing is how a fund that follows the model keeps its actual positions close to those targets, and its rules are set for each fund. For the Global fund, an equally weighted fund with a 2% target weight:

1. **Name changes, at each recalculation.** The fund sells the names the model has sold and buys the model's new names at the target weight, in the same bucket. Cash left over goes into the positions furthest below target, and any shortfall is raised from the positions furthest above.
2. **Position limits, every two weeks.** Any position below 1.5% or above 2.5% of net assets is traded back to 2%. Positions within those limits are left alone.
3. **Deposits and withdrawals.** Deposits go into the positions below target, largest shortfall first. Withdrawals come from the positions above target, largest excess first.
4. **Quarterly realignment.** In the first week of each calendar quarter, when the market split is recalculated, every position is traded back to its target, and the number of names in each bucket follows the new split.
5. **Minimum trade size.** Orders below 0.02% of net assets are skipped.

Trading back only the positions outside the limits, and using deposits and withdrawals to correct the rest, keeps the fund close to its target weights with fewer trades than restoring every position at each recalculation.

## 11. ESG screen

ESG data comes from FMP (ESG disclosure scores and the ESG risk rating), refreshed weekly. A company is **ESG-qualified** when every available factor passes:

| Factor | Passing |
|--------|---------|
| ESG risk rating | AAA to B |
| ESG score | ≥ 50 |
| Governance score | ≥ 50 |

A company with no ESG data is not qualified. The screen applies only to funds configured as ESG-only.

## 12. Audit trail

Every fund recalculation stores its full calculation: for every ETF the fund could draw on, every company held, its market cap, ETF weight, reference weight (and which benchmark it came from, on which date), active weight, rank, and why it did or didn't become a best idea (selected; another region than the ETF's; not overweight; above the 20% limit; beyond the top 10; no validated market cap; quarantined). This is recomputed independently and cross-checked against the stored best ideas.

For any fund and date, a **methodology export** can produce a set of spreadsheets tracing the fund from the ground up: each ETF's holdings and active weights, the benchmark, the combined best ideas, and the fund's holdings, buys and sells.

A **simulation** replays the same rules week by week over the holdings collected so far, using each date's own market caps and benchmarks, to show how a fund would have been built and changed from a chosen start date.

## 13. Design decisions and known limitations

**Design decisions**

- **Whole-company cap, not float-adjusted, for benchmark weights.** Managers size positions against a company's full size; a float-adjusted benchmark would make closely held companies look like overweights for every manager. A float-weighted version was tested and reverted. Float is used only as a minimum (10%) for membership.
- **Market-relative size lines** rather than fixed dollar thresholds, so "large cap" means the same thing in every market environment.
- **Consensus before single conviction.** A company's score adds up every manager that ranks it, each rank worth half the one above, so agreement across managers weighs as much as the strength of one manager's view, and marginal differences between neighbouring ranks matter less. Ranking by best rank alone (the earlier rule) let one manager's #1 outrank ten managers' #2 and turned small active-weight differences into portfolio changes.
- **Managers selected by holdings behaviour.** Funds are kept or excluded by what they hold – derivatives, sector concentration against the benchmarks, country concentration, benchmark coverage – never by their name, except a fund named after an index (it follows that index) and an equal-weighted fund (it says so and its holdings show it), at round levels rather than levels fitted to the funds of the day ([§3](#how-the-selection-rules-are-measured-and-set)).
- **Every manager counts equally**, regardless of assets under management or holdings date – the method aggregates opinions, not money.
- **Global managers as two regional managers.** A Global fund's US and International holdings are each measured as a fund of that region, at the manager's weights within the region ([§8](#8-measuring-a-managers-best-ideas)), so its region allocation doesn't move its stock ideas and its lists compare with the regional managers'. Each part must hold the 20 stocks a regional fund needs.
- **The market split from our own benchmarks.** Measuring the split on the benchmarks the best ideas are measured against keeps the fund consistent across region, style and market, and it follows the market every quarter without a manual input. It's our definition – our large-cap lines, float factors from the index funds and the data vendor – not a published index weight. The float factors are current ones: older splits can't be re-measured on the float of their day.
- **US companies measured against their own style.** A manager holding both styles is measured against two benchmarks in which large companies weigh more than in the blend (×2.3 for value, ×1.75 for growth), so its largest holdings rank somewhat lower than against the whole US market. Accepted, in exchange for not crediting out-of-style mega-caps as convictions.
- **International companies measured against their own market.** The same for developed and emerging markets: a manager holding both – most International managers hold some emerging-market companies – is measured against two benchmarks, so its largest holdings rank somewhat lower than against the whole International market. Accepted, in exchange for measuring emerging-markets managers on their stock picks, which let them into the universe.
- **Reference and mandate kept apart.** Which benchmark a company is measured against follows the company's own style and market; the fund's style and market settings – a filter or a split of its holdings – are separate construction choices. A fund without a style split still measures its US ideas against their style peers; a fund with one fills each style from the ideas measured that way.

**Limitations**

- **Holdings lag.** Although active ETFs publish daily, the data vendor refreshes most funds weekly; ideas are typically several days old, and up to two weeks, when used.
- **New funds join late.** A fund appears on the active list only after its trust's first N-CEN filing after launch (up to a year), and the list is refreshed monthly.
- **Active ETFs only.** Mutual funds and separately managed accounts, which hold most active assets, are not covered; the manager population is younger and skewed toward sponsors that have launched ETFs.
- **Vendor data.** Holdings, prices, share counts, style factors and ESG come from one vendor (FMP); the validation in [§5](#5-company-data-and-data-quality) catches known failure modes but cannot rule out every error. Lines that cannot be identified (a small share of weight, mostly emerging-market names) take no part.
- **No International style split** – International value / growth funds rely on style classification without an International style benchmark (International is split by market instead).
- **Frontier markets unclassified.** A company neither international index fund holds, in a country they don't cover (Kazakhstan, Argentina), has no developed / emerging market: it is measured against the whole International benchmark and is in no market bucket.
- **One firm, several managers.** Funds from the same firm – a Global, a US and an International fund drawing on one research team – each count as a manager, so a firm with several qualifying funds has several voices.
- **Weighting is heuristic.** Square-root market-cap weighting with a 10% cap / 1% floor is a rule, not an optimisation; no risk model, transaction costs or liquidity constraints are applied.
- **No performance claim.** This document describes how the funds are constructed; it makes no claim about their future performance.

## 14. Glossary

| Term | Meaning |
|------|---------|
| **Active weight (Δ)** | A company's weight in the ETF minus its weight in the reference. |
| **Appearances** | Number of managers ranking a company among their best ideas. |
| **Best idea** | One of the top 10 positive active weights of an ETF in a week. |
| **Breakpoint** | The company size at which the market's largest companies reach a given share of total market value. |
| **Bucket** | A region (and, with a market split, a market; with a style split, a style) of a fund, filled from its own ordered list up to its share of the holdings. |
| **Company market cap** | The whole company's equity value, all share classes, in USD. |
| **Consensus score** | The sum of the rank points a company earns from every manager ranking it. |
| **Developed / emerging market** | An International company's market in FTSE's classification, the index family behind the international index funds (Korea developed). |
| **Float factor** | The investable share of a company's equity, from the index funds' holdings or free float. |
| **Half** | In a staggered fund, one of the two groups its holdings are divided into. The halves are reviewed in turn, so each holding is reviewed once per recalculation period. |
| **Keep buffer** | How far below its bucket's cut-off a holding may fall and still be kept, as a multiple of the bucket's share (default 1.25). At 1.0 a holding is kept only while it is within its bucket's top list. |
| **Market split** | A fund's division of its holdings by the float-adjusted weights of the reference benchmarks (US / International; growth / value within the US; developed / emerging within International), recalculated quarterly. |
| **Primary listing** | The listing that represents a company: its home-market ordinary shares. |
| **Rank** | A company's position in a manager's best-ideas list (1 = largest overweight). |
| **Rebalancing** | Trading by a fund that follows a model portfolio to keep its positions close to their target weights. The model portfolio itself doesn't rebalance. |
| **Recalculation** | A fund's periodic review of its model portfolio: its sells, buys and target weights, from the latest best ideas. |
| **Source ETF** | The ETF with the largest active weight in a fund holding. |
| **Target weight** | A holding's weight in the model portfolio, set at each recalculation by the fund's weighting scheme (equal or market-cap based). |

---

## Appendix A – Parameter reference

### Manager selection

| Parameter | Value |
|-----------|-------|
| SEC active list | Last filing within 15 months |
| Equity fund | At least 75% of holdings in stocks |
| Region: US / International / Global | ≥ 80% / ≤ 20% US / in between (measured in a US and an International part) |
| Size lines (fund profile) | 70% (large) / 90% (small) |
| Index name | No index provider's brand or "index" in the fund's name (the fund company's name aside) |
| Holdings transparency | Excluded when a prospectus of its own from the last 15 months carries the non-transparent ETF legend |
| Stocks | 20–200 (US) / 20–250 (International), a depositary receipt counting as its company's stock; Global: each part as its region |
| Derivatives | Excluded for good once holdings show an option or an equity-linked note (note level ≥ 20%) |
| Sector concentration | No sector > 20 points above its highest weight in the US value / US growth / International developed / International emerging benchmarks |
| Top country | ≤ 50% (US funds: the US exempt; Global funds: of the International part, the US aside) |
| Emerging-market share | No limit (≤ 35% until November 2026) – measured against the emerging benchmark |
| Benchmark coverage | ≥ 70% to enter / ≥ 65% to stay (Global funds: each part against its region's) |
| Equal weighting | Excluded when the fund says it weights equally and its 90th-percentile position is under 2× its 10th-percentile one |
| Holdings freshness | Latest holdings ≤ 14 days old |

### Company data and large cap

| Parameter | Value |
|-----------|-------|
| Company region: held by both index markets | One market's float-adjusted holding ≥ 2× the other's decides |
| Market cap validation | 21 days; excluded after 5 days without a good value |
| Large cap: US / International coverage | 93% / 80% of the market |
| Large cap: minimum float | 10% |

### Style and market

| Parameter | Value |
|-----------|-------|
| Fund style: value / growth | ≥ 60% of style-classified weight |
| Market: held by both international index funds | One fund's float-adjusted holding ≥ 2× the other's decides |

### Best ideas

| Parameter | Value |
|-----------|-------|
| Minimum priced holdings | 90% |
| Market cap window | ± 5 days of the holdings date |
| Maximum active weight | 20% |
| Best ideas per ETF | 10 |

### Funds

| Parameter | Value |
|-----------|-------|
| Rank points, ranks 1–10 | 512, 256, 128, 64, 32, 16, 8, 4, 2, 1 |
| Market split (US / International; growth / value, US; developed / emerging, International) | Float-adjusted reference benchmark totals, recalculated quarterly – 65.0% / 35.0%; 57.5% / 42.5%; 73.5% / 26.5% (November 2026) |
| Staggered recalculation | A fund setting: two halves reviewed on alternate weeks, each every recalculation period – the Global fund (two-week period, a half every week) |
| Keep buffer | A fund setting: a holding is kept while in the top *buffer* × its bucket's share of holdings, by score – default 1.25; the Global fund 1.0 (exactly the top list) |
| Market-cap weighting: exponent / cap / floor | 0.5 (square root) / 10% / 1% |
| ESG: risk rating / ESG score / governance | AAA–B / ≥ 50 / ≥ 50 |
| Weekly generation | Wednesday on Tuesday's close (moved around NYSE holidays) |

### Rebalancing (Global fund)

| Parameter | Value |
|-----------|-------|
| Position limits | 1.5–2.5% of net assets, checked every two weeks; outside them traded back to 2% |
| Full realignment | Quarterly, every position back to its target |
| Minimum order | 0.02% of net assets |
