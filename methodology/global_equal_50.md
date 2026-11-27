# Global Large Cap Best Ideas

This document describes how the fund is built and maintained. It covers which managers the fund follows, where its data comes from, how each manager's best ideas are measured, and how those ideas become a portfolio of 50 large-cap companies at equal weight. The holdings are split between the US and international markets. The US holdings are then split between growth and value companies, and the international holdings between developed and emerging markets. Every split follows each part's weight in the market.

Figures quoted "as of" a date illustrate the rules at that date; they are not parameters. The parameters are listed in [Appendix A](#appendix-a--parameter-reference).

---

## Contents

1. [Background](#1-background)
2. [Data sources](#2-data-sources)
3. [The managers](#3-the-managers)
4. [Holdings](#4-holdings)
5. [Company data and data quality](#5-company-data-and-data-quality)
6. [Large cap and the reference benchmarks](#6-large-cap-and-the-reference-benchmarks)
7. [Each company's peers: style and market](#7-each-companys-peers-style-and-market)
8. [Measuring a manager's best ideas](#8-measuring-a-managers-best-ideas)
9. [Building the portfolio](#9-building-the-portfolio)
10. [Maintaining the model portfolio](#10-maintaining-the-model-portfolio)
11. [Audit trail](#11-audit-trail)
12. [Design decisions and known limitations](#12-design-decisions-and-known-limitations)
13. [Glossary](#13-glossary)
- [Appendix A – Parameter reference](#appendix-a--parameter-reference)

---

## 1. Background

An active manager's portfolio mixes two things: positions held because the manager has a view, and positions held to stay close to a benchmark, control risk or stay diversified. The positions the manager holds **well above their market weight** are the ones they are most willing to be wrong about – their highest-conviction calls, their *best ideas*. Academic work on mutual funds (Cohen, Polk and Silli, *Best Ideas*, updated 2021) found that managers' largest overweights outperformed, while the rest of the portfolio largely did not.

The fund follows the managers of US-listed, actively managed equity ETFs with a broad, diversified mandate in US, international or global stocks. Each manager's overweights are its candidate ideas, and each is measured against the company's own peers in the large-cap market. The fund holds the companies that the most managers rank most highly.

Active ETFs are used rather than mutual funds because they publish their holdings far more often (most daily; we read them weekly – [§4](#4-holdings)), so the ideas are recent rather than a quarter old.

### Strategy

| | |
|---|---|
| **Universe** | Large-cap companies worldwide |
| **Regions** | By number of holdings: each region's float-adjusted weight in the reference benchmarks, recalculated quarterly – 65.0% US / 35.0% international as of November 2026, so 33 and 17 |
| **Source of ideas** | US companies: qualifying actively managed US equity ETFs and the US holdings of global ones. International companies: qualifying actively managed international equity ETFs and the international holdings of global ones |
| **Best ideas measured against** | US companies: the US Large Cap Value benchmark for value companies, the US Large Cap Growth benchmark for growth companies. International companies: the International Large Cap Developed benchmark for developed-market companies, the International Large Cap Emerging benchmark for emerging-market companies |
| **Ideas used** | Each manager's ten best ideas, scored by rank and summed across managers |
| **Style** | US holdings by number: each style's float-adjusted weight in the US value and growth benchmarks, recalculated quarterly – 57.5% growth / 42.5% value as of November 2026, so 19 and 14 of the 33. International holdings: no style split |
| **Developed / emerging** | International holdings by number: each market's float-adjusted weight in the International developed and emerging benchmarks, recalculated quarterly – 73.5% developed / 26.5% emerging as of November 2026, so 13 and 4 of the 17 |
| **Holdings** | 50 |
| **Weighting** | Equal (2% each) |
| **Recalculation** | Weekly, staggered: half the holdings reviewed each week, so each holding is reviewed every two weeks against its part's current top list (keep buffer 1.0) |

**Weekly timing.** Every Wednesday, the benchmarks, the best ideas and the fund are recalculated on **Tuesday's closing data**. All of these use the same close, and a market closed that day (a local holiday) uses its last close. When Tuesday is an NYSE holiday, the weekly run moves a day later (Thursday, on Wednesday's close), or else a day earlier, staying in the same week. Runs are counted in calendar weeks, so a moved run never makes the fund miss a week.

| Step | What happens | Cadence |
|------|--------------|---------|
| Managers | Every ETF that tells the SEC it is not an index fund; its profile decides whether it is a diversified US or international equity fund that qualifies ([§3](#3-the-managers)) | Monthly list, rules re-checked daily |
| Holdings | Each qualifying ETF's holdings, every line identified as a company ([§4](#4-holdings)) | Daily (Tue–Sat); most ETFs refresh weekly |
| Company data | Prices, market caps, share-class consolidation, region, validation ([§5](#5-company-data-and-data-quality)); each US company's style and each international company's market ([§7](#7-each-companys-peers-style-and-market)) | Daily |
| Large-cap lines and benchmarks | Where large cap ends in each region; the reference benchmarks ([§6](#6-large-cap-and-the-reference-benchmarks)) | Weekly |
| Best ideas | Top 10 active weights per ETF ([§8](#8-measuring-a-managers-best-ideas)) | Weekly |
| Portfolio | Candidate lists per part → US growth + US value + international developed + international emerging holdings (19 + 14 + 13 + 4) → buys and sells ([§9](#9-building-the-portfolio), [§10](#10-maintaining-the-model-portfolio)) | Weekly, each holding reviewed every two weeks |

## 2. Data sources

| Source | Used for |
|--------|----------|
| **SEC EDGAR – Form N-CEN** | Which ETFs are actively managed: every registered fund's annual report of its own type ([§3](#3-the-managers)). Read directly from EDGAR, the day after filing. |
| **SEC EDGAR – fund prospectuses** | Which ETFs are non-transparent: the legend the SEC requires on their prospectus cover, found by EDGAR's full-text search ([§3](#3-the-managers)). Checked monthly. |
| **Financial Modeling Prep (FMP)** | ETF holdings and profiles (asset class, sector weights, sponsor, AUM); company profiles (identifiers – ISIN, CUSIP, SEC CIK – name, domicile, sector, currency, trading status); daily prices and market caps with history; shares outstanding and free float; quarterly financials (share counts); fundamental factors (growth, margins, valuation ratios, yields) for style classification; FX rates; the stock screener (all large companies, across about 40 exchanges); NYSE holiday calendar. |
| **Vanguard index funds – VTI, VEA, VWO** (holdings via FMP) | The whole market: CRSP US Total Market (VTI) for the US; FTSE Developed All Cap ex US (VEA) and FTSE Emerging Markets All Cap China A Inclusion (VWO) for international. They hold nearly every listed company at its float-adjusted weight, so they define where large cap ends and each company's float ([§6](#6-large-cap-and-the-reference-benchmarks)). |
| **Style reference ETFs** – Vanguard VUG / VTV / VBK / VBR, iShares Morningstar ILCG / ILCV / IMCG / IMCV / ISCG / ISCV (holdings via FMP) | Which US companies the index providers (CRSP, Morningstar) classify as value or growth ([§7](#7-each-companys-peers-style-and-market)). |

All market data comes from APIs; nothing is scraped. Every figure is converted to **USD at that date's exchange rate**, from the currency the listing actually trades in.

## 3. The managers

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
| Option income | Name (covered call, premium income, buy-write, 0DTE…) | No – returns driven by the option overlay (funds not named as such are caught by the derivatives rule below) |
| Fixed income, multi-asset, alternative, commodity, currency | FMP asset class | No |
| **Equity** | Equity asset class (or none) and **at least 75% of holdings in stocks** | **Yes** |

The equity test reads the holdings themselves: cash, currencies, FX forwards, money-market funds, derivatives, bonds, equity-linked notes, private placements and other funds don't count as stock.

### The fund's profile

Each equity fund is profiled from its holdings against our company data and the total-market index funds:

| Measure | How |
|---------|-----|
| **Region** | Share of the fund in US companies (by each company's region – [§5](#5-company-data-and-data-quality)). ≥ 80% US → a US fund; ≤ 20% → an international fund; in between, a global fund. |
| **Size** | Each stock large above its market's 70% breakpoint, small below the 90% one, mid between (Morningstar's approach). The fund is *large* with ≥ 70% large stocks, *small* with ≥ 50% small, *mid* with ≥ 50% mid, *smid* with ≥ 70% mid + small, otherwise *all-cap*. |
| **Stock count** | Lines matching a stock the total-market index funds hold – directly, or through the company: a depositary receipt (ADR) counts as its company's stock, which the index funds hold in its home market. |
| **Country concentration** | Our companies' country of domicile: the largest country. |

### Selection rules

A profiled equity fund is one of the fund's managers (**active**) when it passes every rule. The rules keep out funds whose overweights would reflect a mandate rather than stock selection within a broad large-cap market: derivative strategies, funds that follow an index, equal-weighted funds, funds that don't publish their holdings daily, sector and thematic funds, single-country funds, and funds investing mostly outside large caps. They are applied every time holdings are read, so a manager that drifts out of its mandate drops out on its own and returns when it comes back – except one that has used derivatives:

| Rule | Passes when | Why |
|------|-------------|-----|
| Index name | No index provider's brand (Dow Jones, S&P, MSCI, FTSE, Russell, STOXX, Nasdaq and the like) or the word "index" in the fund's name – the fund company's own name aside | A fund named after an index follows it, even when it reports to the SEC as actively managed: its overweights are the index's screen, not a manager's choices. |
| Holdings transparency | The fund publishes its holdings daily: no prospectus of its own from the last 15 months carries the SEC's non-transparent ETF legend ("This ETF is different from traditional ETFs…") | A non-transparent fund discloses its actual holdings only quarterly, a month or two late; the data vendor re-dates that file with every refresh, so its ideas would be last quarter's positions. The fund's annual SEC filing has a related question, but it is answered unreliably; the prospectus legend is required and dropped when a fund converts. |
| Region | A US fund (≥ 80% US), an international fund (≤ 20% US) or a global fund (in between) | Each manager's overweights are measured against one market – a global fund's in two parts, its US companies and its international ones ([§8](#8-measuring-a-managers-best-ideas)). |
| Size | Large or all-cap | Small- and mid-cap managers' overweights are measured against a different opportunity set. |
| Number of stocks | 20 to 200 (US), 20 to 250 (international); a global fund 20 to 200 US **and** 20 to 250 international companies | Fewer than 20 is a concentrated fund where every holding is an overweight; above the maximum the fund is close to an index and its tilts are small. International managers spread over more markets and names. A global fund's part of a few stocks would make nearly all of them best ideas, so a fund short in either region is excluded. |
| Derivatives | The fund's holdings have **never** shown an option contract or an equity-linked note | Covered-call and option-income funds hold ordinary-looking stocks, but their weights follow the option book; once seen, the fund is excluded for good. |
| Sector concentration | No sector more than **20 points** above its reference weight: the highest weight the sector has in the US large value, US large growth, international large developed or international large emerging benchmark | A sector or thematic fund's overweights reflect its mandate, not stock selection; measuring each sector against the market lets a growth manager hold as much technology as the growth market does. |
| Top country | ≤ 50% of an international fund, or of a global fund's international holdings | Single-country funds (Japan, India, China, Korea – 80–100% in one country) – same reason. Half rather than a third: a diversified emerging-markets fund holds 25–45% in Taiwan or China, as the emerging benchmark does. |
| Benchmark coverage | ≥ 70% of the fund's stock weight in large-cap companies of its own region ([§6](#6-large-cap-and-the-reference-benchmarks)) to enter; ≥ 65% to stay – a global fund in each region | Holdings outside large cap have no benchmark weight, so they would all count as overweights. |
| Equal weighting | Not an equal-weighted fund: the fund says it weights its holdings equally (in its name or description) **and** its holdings are flat – its 90th-percentile position under **2×** its 10th-percentile one | An equal-weighted fund's weights come from its method, not the manager's view of each stock: its overweights would simply be its smallest companies. Both signals are needed – descriptions can be wrong, and many concentrated managers size their picks alike by choice. |
| Holdings freshness | Latest holdings ≤ 14 days old | Ideas must be current. |
| Duplicate | Only one row per fund | A fund moved to a new trust is briefly listed twice under one ticker; it must count once. |

Anything unknown fails its rule; the sector, coverage, global stock-count and equal-weighting rules are measured on the fund's holdings, so a new fund waits for its first holdings. A fund enters the coverage rule at 70% and stays while at 65%, so a manager near the line doesn't drop in and out. Funds are judged by what they hold, never by their name – except a fund named after an index, which follows it, an equal-weighted fund, which says so and whose holdings show it, and a non-transparent fund, whose prospectus says so – and the levels are round numbers rather than ones fitted to the funds of the day. US managers supply the fund's US ideas, international managers its international ideas, and global managers both – each region's from its holdings in that region.

## 4. Holdings

**Frequency and dating.** Holdings are read from FMP every weekday run. FMP refreshes most funds every trading day, some a few times a week or weekly; each holding set is stored under FMP's own date. FMP publishes a day's file during that day's US session, after the early-morning run, so each run uses the previous day's file, which reflects the fund's positions one to two trading days earlier. A fund's holdings are used for up to 14 days.

**Identifying each line.** Every holding line is matched to a company:

- by its symbol, but only when the security behind that symbol agrees with the line's ISIN / CUSIP or name (a holding's symbol can belong to a different security elsewhere);
- else by ISIN;
- else by name, accepting a candidate only when every meaningful word of the shorter name matches the longer one – never on a single shared word.

Lines that aren't stocks are recognised and excluded from the calculation: cash and sweeps, currencies and FX legs, money-market and other funds, derivatives and options, bonds, equity-linked notes (which often carry their issuer's symbol – an ELN on JPMorgan is not JPMorgan stock), rights, warrants and private holdings.

**Duplicate lines.** When a fund lists the same company on several lines (separate lots), they are summed when they imply the same share price; when the prices disagree they are most likely different securities mis-identified as one, and that company is **quarantined** – left out for that fund that week – rather than risk a wrong weight.

## 5. Company data and data quality

### One company, one weight

A company can trade as several listings – share classes (Alphabet GOOGL / GOOG), foreign listings (Alphabet on XETRA), depositary receipts (TSMC's US ADR), and home-market lines in two currencies (Tencent in Hong Kong dollars and renminbi). Each would otherwise be weighted separately and could be bought twice. All listings are grouped into **companies** using the SEC CIK, shared ISINs, matching names in the same domicile, depositary-receipt names and matching market caps (with safeguards against vendor errors, e.g. a wrong ISIN attached to an unrelated listing). A manager's exposure to all of a company's listings is summed, and the company is measured and held once.

**Company market cap** = the whole company's equity value: every share class, listed or not (as reported by FMP on the company's primary listing), in USD. Listings are never summed – each already carries the whole company's cap.

**Primary listing** – the listing that represents the company: an active, ordinary-share line (not preferred, note, unit, depositary or thinly traded line), on its home exchange, then a US listing, then other markets; in its exchange's own currency.

**Region** – a company is **US** or **international** by the first of these that decides:

1. **The index providers' own country assignment** – a company held by the US total-market index fund (CRSP) is US; one held by the developed or emerging ex-US index funds (FTSE) is international. Held by both, the market holding it at least twice the other's float-adjusted value decides (the data vendor occasionally files a foreign stock under a US company's identifiers); a genuine dual holding (Waste Connections, James Hardie) goes to the next rule.
2. **The company's head-office country**, unless it is a tax haven (Bermuda, Cayman Islands, British Virgin Islands, the Channel Islands and similar). This settles the companies the indexes leave out: foreign companies listed only in the US (Arm, Spotify, MercadoLibre, NXP, Nu, Sea are international) and some US ones (partnerships, business development companies, very low-float companies, new listings – US).
3. **The country of incorporation** of an ordinary listing, unless a tax haven.
4. **The listing exchange** – US when the company's main listing is on a US exchange.

So TSMC, ASML, Shell, Shopify, PDD, Trip.com and Spotify are international (whatever their US listings), while Accenture, Medtronic, Linde and Chubb – incorporated abroad, run from the US and held by the US index – are US. The same rule is used everywhere: manager profiles, the benchmarks and both regions of the fund.

### Validated prices and market caps

Market caps decide which companies are large cap and feed every benchmark weight, so they are validated rather than taken as quoted:

- **Cross-check against history.** Each day's value is checked against the stored values for the previous three weeks; a disagreement withholds the new value (a vendor revision is recognised and accepted, and so is a share-count update after a filing: market caps up to 15% off with every price agreeing). A listing without a good value for 5 days is excluded, and re-checked every week, so an exclusion lasts only while the disagreement does.
- **Glitch repair.** Vendor market-cap histories occasionally jump by a wrong unit (pence vs pounds) or share count for days or months. A cap that moves threefold in a day without its price moving is a glitch; the listing's current share count arbitrates which side is wrong, and the bad values are repaired at price × the correct share count.
- **Share-count verification.** When the quoted share count and the history disagree (typically after a merger or buyback), a second source – quarterly financials, or the total-market index funds' holdings – decides which is right before the history is rewritten. The index funds hold only the free float, so they can show that a share count is too low (they hold more shares than the history says exist) but never that it is too high; they are used only that way.
- **Index cross-check.** An index fund cannot hold more of a company than exists, so a large-cap company the index funds value at ≥ 1.15× our cap is flagged and its share count re-verified before the benchmarks are formed.

### Float

FMP's free float and the total-market index funds' holdings give each company a **float factor** (the investable share of its equity; China A shares, held by FTSE at a quarter of their float, are scaled up accordingly). It is not used to weight anything ([§12](#12-design-decisions-and-known-limitations)), but a company needs **at least 10% float** to count as large cap – S&P's minimum. It also identifies exchange-traded notes and preferreds named like their issuer (zero equity float), which are never treated as the company.

## 6. Large cap and the reference benchmarks

### Where large cap ends

"Large cap" is defined the way the index providers define it – as the **largest companies making up a given share of the market** – rather than at a fixed dollar line, which drifts with markets and means different things in different markets.

**How the line is drawn.** Each week, for each market:

1. **Take the whole market** – what the Vanguard total-market index funds hold: VTI (CRSP US Total Market) for the US; VEA + VWO (FTSE Developed ex US and FTSE Emerging, China A shares included) for international. Between them they hold nearly every listed company.
2. **Measure each company's investable size.** A company's float-adjusted market value – the value of its shares actually available to investors – is read from the index fund's position in it: the fund holds every company in proportion to its float, so its position, scaled up to the whole market, gives the company's float-adjusted value. Several listings or share classes of one company are added together, and a company held by both markets counts as US. China A shares, which the index holds at only a quarter of their weight, are scaled up to their full size first.
3. **Line the companies up from largest to smallest and keep a running total** of their float-adjusted value, as a share of the whole market's.
4. **The breakpoint at a coverage *c* is the size of the company at which the running total reaches *c*.** Every company at least that large is in the market's top *c*.

The lines therefore move with the market: when markets rise, so do the lines, and "large cap" keeps meaning the same slice of each market. As of September 2026:

| Coverage | US | International |
|----------|----|---------------|
| 70% | $94B | $15B |
| 80% | $50B | **$7.4B – International benchmark** |
| 90% | $16B | $3.1B |
| 93% | **$10.1B – US benchmark** | $2.2B |

- **US large cap**: 93% coverage.
- **International large cap**: 80% coverage.

**Why 93% for the US.** The Russell 1000 – the large-cap index US large-cap managers are most often measured against, alongside the S&P 500, which covers much the same companies – makes up about 93% of the investable US market. Cutting the US benchmark at 93% coverage therefore gives the universe US large-cap managers compare themselves with, and measures their overweights against it.

**Why 80% for International.** The international index providers draw two main lines: MSCI and FTSE call the top 70% of each market *large cap*, and the top 85% *large + mid cap* (MSCI's Standard indexes, such as MSCI EAFE and MSCI ACWI ex US, the benchmarks international large-cap managers usually report against). 80% is the mid point between those two options (85% is about $5B in September 2026; the others are in the table above). It also puts International large cap reasonably close in size to the US line (about $10.1B at 93%). The same 93% coverage would not: the international market is spread over many more companies in many smaller markets, so 93% of it reaches down to about $2B.

**Applying the line to a company.** A company is **large cap** when its **whole market cap** – every share class, listed or not, not only the floating part – is at or above its region's breakpoint, and at least **10% of it floats**. This is the S&P / Russell convention: the line is drawn on the investable market, a company is measured on its whole size, and a minimum float keeps out companies investors can barely buy. To be sure no company that could pass is missed, the weekly screen of listed companies goes down to 80% of the lower of the two lines.

### The reference benchmarks

Four benchmarks are rebuilt weekly from the full screened universe (every listed company above the screening threshold, across about 40 exchanges):

| Benchmark | Members | Used for |
|-----------|---------|----------|
| **US Large Cap Value** | The US large-cap companies classified value ([§7](#7-each-companys-peers-style-and-market)) | US value companies |
| **US Large Cap Growth** | The US large-cap companies classified growth | US growth companies |
| **International Large Cap Developed** | The international large-cap companies of developed markets ([§7](#7-each-companys-peers-style-and-market)) | International developed-market companies |
| **International Large Cap Emerging** | The international large-cap companies of emerging markets | International emerging-market companies |

Within each benchmark, **weights are proportional to the whole company's market cap** – the size the managers themselves see and size positions against ([§12](#12-design-decisions-and-known-limitations)) – and add up to 100%. Each US company belongs wholly to one style, each international company wholly to one market. Emerging markets are about 35% of the international large-cap market by whole market cap (26.5% float-adjusted, November 2026).

## 7. Each company's peers: style and market

Each company is compared with its peers, which are the US companies of its style for a US company and the international companies of its market for an international one. The peer group decides two things: the benchmark the company's active weight is measured against ([§8](#8-measuring-a-managers-best-ideas)), and the part of the fund it competes for ([§9](#9-building-the-portfolio)).

US companies are grouped by style and international companies by market. The index providers' style classifications used below cover US companies only, and a large part of the international market has no reliable style. Every international company's market, by contrast, is known from the index provider.

### US companies: value or growth

Each US company carries one style, **value** or **growth**, from the first of these sources that classifies it:

1. **Index provider classification** – companies held by the style reference ETFs (CRSP via Vanguard, Morningstar via iShares, large / mid / small). Each reference ETF holding a company is one vote for its style; the majority wins, and a tie keeps the existing style (the two families overlap). These are re-read weekly and re-applied daily, so styles follow the index providers' reconstitutions.
2. **Active managers' classification** – a company not in the reference ETFs but held by active ETFs whose own profile is value or growth (at least 60% of their style-classified holdings) takes that style.
3. **Statistical classifier** – for the rest, a gradient-boosting model trained on the reference-classified companies' fundamentals (size, sector, industry, growth rates, margins, P/E, P/B, yields) predicts value or growth.

A US company none of these classify (about 0.1% of the US large-cap market) has no style.

### International companies: developed or emerging

Each international company is **developed** or **emerging** in FTSE's classification, the index family behind the international index funds. Under it, Korea and Poland are developed, while Taiwan, China, India, Brazil and Saudi Arabia are emerging. The market comes from the first of these that decides:

1. **The index provider's assignment** – a company held by the emerging-markets index fund (FTSE Emerging) is emerging, one held by the developed ex-US fund (FTSE Developed) developed; held by both, the one holding it at least twice the other's float-adjusted value decides.
2. **Its country** – a company neither fund holds takes the classification of its country, as the index-held companies of that country have it, so a reclassification by the index provider follows on its own. A tax haven, or a US country of record (a depositary receipt's or a US holding company's), says nothing about the market.
3. **Its country of incorporation**, the same way.

A company none of these decide, such as one in a frontier market like Kazakhstan or Argentina, belongs to neither market.

## 8. Measuring a manager's best ideas

For every active ETF, each week:

**Inputs.** The ETF's latest holdings (≤ 14 days old), each company's market cap on the holdings date (± 5 days). An ETF where fewer than **90%** of the holdings have a validated market cap is skipped for the week – a partial picture would distort every weight.

**Active weight.** For each company the ETF holds, two weights are compared:

- **the manager's weight** – the share of the ETF's stock portfolio invested in the company, across all its listings;
- **the benchmark weight** – the company's share of its reference benchmark: for a US company, US Large Cap Value if it is a value company, US Large Cap Growth if it is a growth company; for an international company, International Large Cap Developed if it is a developed-market company, International Large Cap Emerging if it is an emerging-market one. A company below its region's large-cap line is in none, so its benchmark weight is zero.

The **active weight** is the difference: how much more of the company the manager holds than the benchmark does. For example, an international manager with 3% of its fund in a company that makes up 0.5% of the International Large Cap Developed benchmark has an active weight of 2.5 percentage points – a clear overweight. A company held at or below its benchmark weight is not an overweight at all.

All of a manager's active weights are then ranked together.

**Why US companies are measured against their own style.** A manager's overweight is most meaningful against the peers it competes with for a place in the portfolio. Measured against the whole US large-cap market, a value manager that holds a growth mega-cap at well below its growth-index weight could still show it as one of its largest overweights – a conviction it doesn't have. Against the growth benchmark, where that company weighs more, it shows as what it is. The manager itself needs no style label: each of its holdings is simply compared with its own style's benchmark.

**Why international companies are measured against their own market.** Against the whole international large-cap market, a dedicated emerging-markets manager's largest overweights would simply be the largest emerging-market companies – TSMC, Tencent, Alibaba, the big Indian banks – because the manager holds only emerging markets, which are a minority of the international market: an allocation between developed and emerging markets, not stock selection. Against the emerging benchmark, an emerging-markets manager's overweights are its stock picks within its own market, so emerging-markets managers are among the fund's international managers. A manager holding both markets is measured against both, each holding against its own market's benchmark. A company with no style or market is measured against its whole region.

**Selection.** Companies with a positive active weight are ranked from the largest overweight down. An active weight above **20%** is treated as abnormal (a data problem or an extreme concentrated bet) and dropped. A company from **the other region** – a US manager's holding in TSMC or Shell, an international manager's holding in a US company – counts in the manager's total, but is never one of its best ideas: each part of the fund takes only its own region's companies from its own region's managers, so such an idea could never be bought and would only push a usable one out of the manager's list. The **top 10** per ETF are that manager's best ideas, with their rank (1 = the largest overweight).

**Global managers: two parts.** A global fund is measured as two managers: its **US holdings** and its **international holdings**, each re-weighted to 100% of that part – the manager's weight in the company within the region – and measured exactly as a US or an international fund's holdings are, with its own top 10. For example, a global fund 60% in the US with 3% in Microsoft has 5% of its US holdings in it; 2% in Nestlé is 5% of its 40% international holdings. Its US list then competes in the fund's US parts and its international list in the international part. Measuring the whole fund against one combined world benchmark would skew its ideas three ways:

- **Its region allocation would leak into every stock.** A global manager five points underweight the US would show every US holding a little less overweight and every international one a little more – a call on regions, not on stocks. Within each region the weight is the manager's own, so only its stock selection is measured.
- **The style benchmarks would favour international companies.** US companies are measured against their own style's benchmark, where a large company weighs 1.75× (growth) to 2.3× (value) its weight in the whole US market; international companies against their own market's (developed or emerging). Ranked together, the US holdings would show smaller overweights and the international ones would crowd the top 10.
- **Its ranks would not compare with the regional managers'.** One top 10 across both regions spreads a global manager's ideas over the two regions at lower ranks (2, 5, 9 rather than 1, 2, 3), so its ideas would earn fewer points than a regional manager's. With a list per region, it scores in each region like a manager of that region.

**Companies below the large-cap line.** Such a company has no benchmark weight, so its whole ETF weight is active weight. The fund applies the large-cap test itself ([§9](#9-building-the-portfolio)), so these never enter as new buys.

## 9. Building the portfolio

The fund is built as four parts – **US growth, US value, international developed and international emerging holdings** – the US parts from US managers and global managers' US lists, the international parts from international managers and global managers' international lists.

### The market split

The number of holdings in each part follows the market rather than a fixed number. The fund divides its holdings three ways:

| Split | Divides | Measured on |
|-------|---------|-------------|
| Region | The 50 holdings between the US and international | The US large-cap market against the international large-cap market ([§6](#6-large-cap-and-the-reference-benchmarks)) |
| US style | The US holdings between growth and value | The US Large Cap Growth benchmark against the US Large Cap Value benchmark |
| International market | The international holdings between developed and emerging markets | The International Large Cap Developed benchmark against the International Large Cap Emerging benchmark |

Each split is measured on **the same benchmarks the managers' best ideas are measured against** ([§8](#8-measuring-a-managers-best-ideas)). A growth company is measured against the growth market and competes for a growth position, and the number of growth positions is the growth market's share of the US market. An emerging-market company is treated the same way within the international market. The fund's allocation and its measure of conviction therefore rest on one definition of the market. The managers decide which companies the fund holds, and the market decides how many of each kind.

The values are **float-adjusted**, meaning each company's market cap is multiplied by the share of its stock that floats, as the index providers weight their indexes. On whole market caps, the state, family and cross holdings common outside the US, and especially in emerging markets, would count in full. In November 2026 the US share of the two regions was 58% on whole market caps against 65% float-adjusted, and the emerging share of the international market was about 35% against 26.5%.

The splits are recalculated **quarterly**, on the first weekly run of each calendar quarter, and each recalculation uses the latest one. Each share is applied to a number of holdings and rounded to the nearest whole number (a half rounds up), with the other side taking the rest. The US share is applied to the 50 holdings first, and then the growth share is applied to the US holdings and the emerging share to the international ones. As of November 2026:

| Split | Part | Float-adjusted value | Weight | Holdings |
|-------|------|----------------------|--------|----------|
| Region | United States | $67.8T | 65.0% | 33 of 50 |
| | International | $36.4T | 35.0% | 17 of 50 |
| US style | Growth | $38.9T | 57.5% | 19 of 33 |
| | Value | $28.8T | 42.5% | 14 of 33 |
| International market | Developed | $26.9T | 73.5% | 13 of 17 |
| | Emerging | $9.7T | 26.5% | 4 of 17 |

The fund's four parts are therefore 19 US growth, 14 US value, 13 international developed and 4 international emerging holdings. A company with no style or market belongs to no part and is not held.

### The candidate lists

For each part:

1. **Collect** the best ideas of that region's active ETFs from the last two weeks (the recalculation period): US companies from US managers and global managers' US lists, international companies from international managers and global managers' international lists. A global manager counts once in each region. Each part then keeps its own style's (US) or market's (international) companies.
2. **Filter**: only companies that are large cap on the recalculation date – their whole market cap at or above their region's breakpoint, with at least 10% float ([§6](#6-large-cap-and-the-reference-benchmarks)).
3. **Score each company across managers.** Each of the region's managers gives a company points for its rank on that manager's best-ideas list – all ten ranks count – and the points are summed into the company's **consensus score**:

   | Rank | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 |
   |------|---|---|---|---|---|---|---|---|---|----|
   | Points | 512 | 256 | 128 | 64 | 32 | 16 | 8 | 4 | 2 | 1 |

   Each rank is worth half the one above, so one manager's #1 (512) equals two managers' #2 or eight managers' #4. A manager counts once per company. Alongside the score, each company carries its **best rank** with any manager, its **appearances** (how many managers rank it) and its **largest active weight** among them, with the ETF that holds it (the *source ETF*).
4. **Order**: highest score first, then most appearances, then largest active weight. Every manager counts equally, whatever its size or the date of its holdings within the two weeks.

So a stock that is the #2 of three managers (768) comes ahead of one that is the #1 of a single manager (512), and a stock ranked #4 to #10 by many managers can outscore one ranked #3 by only one. The ranking deltas within a manager's list, which can be marginal, matter less than how many managers agree.

5. **The ideal portfolio** is the top of each list up to its part's share of the holdings (the top 19 US growth, 14 US value, 13 international developed and 4 international emerging companies at the splits above), each part ordered by its own scores: US growth companies against each other, emerging-market companies against each other. A part with fewer eligible ideas than its target holds fewer; the shortfall is not made up from another part – except that an emerging part short of ideas leaves its places to the developed part.

### Weighting

Every holding has the same **target weight**: 1/50, or 2%. The weights are set at each recalculation; the model portfolio doesn't track how they drift with prices in between. So the split by number of holdings is also the split by weight (66% US / 34% international at 33 / 17; 38% US growth / 28% US value at 19 / 14; 26% international developed / 8% international emerging at 13 / 4 – rounding to whole holdings keeps each within one holding, 2%, of the market split). If fewer than 50 eligible ideas exist on a recalculation date, the fund holds what there is, still equally weighted.

## 10. Maintaining the model portfolio

This section describes the **model portfolio**: the list of holdings and their target weights that the method produces each week. Its buys and sells are changes to that list, not trades. A fund that follows the model trades to match it, and keeps its positions close to their target weights as prices move, by the rebalancing rules at the end of this section.

The model portfolio is recalculated every week. Each week, half of its holdings are reviewed against their parts' current top lists. Those that have dropped out are sold, the free places are filled with the best ideas not yet held, and every holding is given the same target weight. Because the halves take turns, each holding is reviewed every two weeks.

### The two halves

When the model portfolio starts, each part's holdings are divided between two halves by alternating down the part's ranking: ranks 1, 3, 5 … go to one half and ranks 2, 4, 6 … to the other. A new holding joins the half of the holding whose place it takes. The two halves are reviewed on alternate weeks.

### The weekly recalculation

Each week, in this order:

1. **Sell holdings that are no longer a best idea.** A holding that is no longer among the best ideas of any of its region's managers is sold, whichever half it is in.
2. **Sell holdings of the half under review that have dropped out of their part's top list.** Each part's top list is its target number of companies by score: at the November 2026 targets, the top 19 US growth, 14 US value, 13 international developed and 4 international emerging companies. A holding of the other half that has dropped out is kept until its own half's week.
3. **Buy.** Each part fills its free places up to its target with its highest-scoring ideas not yet held. A sold US value holding is therefore replaced by a US value idea, and a sold emerging-market holding by an emerging-market idea. A holding of the other half kept under step 2 still takes a place, so its part buys one company fewer that week. A part short of eligible ideas holds fewer than its target, and the shortfall is not filled from another part, except that an emerging part's empty places go to the developed part.
4. **Set the target weights.** Every holding is given the same target weight, 1/50 or 2%.

For example, suppose a US value holding in the first half falls to 16th on the US value list in a week when the second half is under review. It stays in the model portfolio that week, and the 14th-ranked US value idea, which would otherwise have taken its place, is not bought yet. The next week the first half is reviewed. If the holding is still outside the top 14, it is sold and the highest-scoring US value idea not held is bought.

### Why the model portfolio works this way

A holding is kept only while it is within its part's top list, a keep buffer of 1.0. A wider buffer would keep a holding a few places below its cut-off. That would mean fewer changes to the list, and fewer trades for a fund that follows it, but the holdings it keeps are the ones whose managers' consensus has weakened. The model portfolio is built to follow the current consensus, so it holds each part's current top list.

The two halves limit the turnover this brings. A holding that drops out of its top list gets until its own half's week to recover, while new ideas can still enter every week, and the changes are spread over every week. Both choices are reviewed as the model portfolio's history accumulates.

### When the split or a company's classification changes

When the quarterly market split changes, each part's target changes with it. A part now above its target sells its lowest-scored holdings beyond the new target as each half comes up for review, and the freed places go to the part below its target. The model portfolio therefore reaches the new split within two weeks.

When a US company's style changes, it moves to the other style's part. It is then measured against that style's benchmark and kept only while it is within that part's top list. An international company that the index provider moves between developed and emerging markets is treated the same way.

### Other rules

- **A holding that falls below its region's large-cap line** is not sold for that reason alone. It stays while it keeps its place in its part's top list, but a company below the line can never be a new buy.
- **Corporate changes** don't change the list. Holdings are tracked by company, so a change in a company's primary listing, for example after a re-listing, doesn't cause a sale and re-purchase.
- **No ideas at all**, for example during a data outage: the model portfolio keeps its holdings unchanged rather than selling them.

Every buy and sell is recorded with its reason, score, best rank, number of managers and contributing ETFs.

### Rebalancing

The model portfolio sets target weights; it doesn't track how positions drift with prices. Rebalancing is how a fund that follows the model keeps its actual positions close to those targets. The fund follows these rules:

1. **Name changes, every week.** Each week the fund trades the model's changes for the half under review. It sells the names the model has sold and buys the model's new names at 2% of the fund's net assets each, in the same part. A name the model sells because it is no longer a best idea is sold in the same week, whichever half it is in. Cash left over goes into the positions furthest below 2%, and any shortfall is raised from the positions furthest above 2%.
2. **Position limits, every two weeks.** Any position below 1.5% or above 2.5% of net assets is traded back to 2%. Positions within those limits are left alone.
3. **Deposits and withdrawals.** Deposits go into the positions below 2%, largest shortfall first. Withdrawals come from the positions above 2%, largest excess first.
4. **Quarterly realignment.** In the first week of each calendar quarter, when the market split is recalculated, every position is traded back to 2%, and the number of names in each part follows the new split.
5. **Minimum trade size.** Orders below 0.02% of net assets ($10,000 for a $50 million fund) are skipped.

Between realignments, positions drift with prices within the 1.5–2.5% limits. Trading back only those outside them, and using deposits and withdrawals to correct the rest, keeps the fund close to equal weight with fewer trades than restoring every position each week.

## 11. Audit trail

Every recalculation stores its full calculation: for every ETF the fund could draw on, every company held, its market cap, ETF weight, benchmark weight (and which benchmark it came from, on which date), active weight, rank (a global ETF's within its US or international part), and why it did or didn't become a best idea (selected; the other region; not overweight; above the 20% limit; beyond the top 10; no validated market cap; quarantined). This is recomputed independently and cross-checked against the stored best ideas.

For any recalculation date, a **methodology export** can produce a set of spreadsheets tracing the fund from the ground up: each ETF's holdings and active weights, the benchmarks, the combined best ideas, and the fund's holdings, buys and sells.

A **simulation** replays the same rules week by week over the holdings collected so far, using each date's own market caps and benchmarks, to show how the fund would have been built and changed from a chosen start date.

## 12. Design decisions and known limitations

**Design decisions**

- **US companies against their own style's benchmark.** Out-of-style mega-caps aren't credited as convictions ([§8](#8-measuring-a-managers-best-ideas)). The trade-off: a US manager that holds both styles is measured against two benchmarks in which large companies weigh more than in the whole US large-cap market (×2.3 for value, ×1.75 for growth), so its largest holdings rank somewhat lower. Accepted.
- **International companies against their own market's benchmark** – developed or emerging – rather than their style, for want of a reliable international style classification (so the international holdings have no style split). Emerging-markets managers are measured on their stock picks, not on their allocation to emerging markets, which is what lets them into the fund's managers. The trade-off, as in the US: a manager holding both markets is measured against two benchmarks, so its largest holdings rank somewhat lower than against the whole international market. Accepted.
- **The developed / emerging split on the same benchmarks as the ideas.** The managers decide which international companies, the market decides how many of each market – so the fund's emerging-market exposure follows the market rather than how many of its international managers happen to invest in emerging markets.
- **The style split on the same benchmarks as the ideas.** US companies are measured against their own style's benchmark and compete for their own style's positions, sized by that benchmark's share of the US market. The fund's growth / value balance follows the market rather than whichever style the managers happen to favour in a given week: the managers decide which companies, the market decides how many of each style.
- **Each region from its own managers.** US ideas come from US managers and international ideas from international managers, each measured against its own market, so the two parts of the fund don't compete on incomparable active weights.
- **Global managers as two regional managers.** A global fund's US and international holdings are each measured as a fund of that region, at the manager's weights within the region, so its region allocation doesn't move its stock ideas and its lists compare with the regional managers'. Each part must hold the 20 stocks a regional fund needs.
- **Whole-company cap, not float-adjusted, for benchmark weights.** Managers size positions against a company's full size; a float-adjusted benchmark would make closely held companies look like overweights for every manager. A float-weighted version was tested and reverted. Float is used only as a minimum (10%) for large-cap membership.
- **Market-relative large-cap lines** rather than fixed dollar thresholds, so "large cap" means the same thing in every market and every market environment.
- **Consensus before single conviction.** A company's score adds up every manager that ranks it, each rank worth half the one above, so agreement across managers weighs as much as the strength of one manager's view, and marginal differences between neighbouring ranks matter less. Ranking by best rank alone (the earlier rule) let one manager's #1 outrank ten managers' #2 and turned small active-weight differences into portfolio changes.
- **Managers selected by holdings behaviour.** Funds are kept or excluded by what they hold – derivatives, sector concentration against the benchmarks, country concentration, benchmark coverage – never by their name, except a fund named after an index (it follows that index) and an equal-weighted fund (it says so and its holdings show it), at round levels rather than levels fitted to the funds of the day.
- **Every manager counts equally**, regardless of assets under management or holdings date – the method aggregates opinions, not money.
- **Equal weight.** Each idea gets the same conviction in the fund, whatever the company's size; the fund carries no market-cap tilt.

**Limitations**

- **Turnover is high.** With a keep buffer of 1.0, every idea that falls out of its part's top list is sold when its half is next reviewed and many return later, so a large share of the holdings changes each year, and the trading cost that comes with it ([§10](#10-maintaining-the-model-portfolio)). Each part fills only its own free positions, so the split stays exact; a part can only fall below its target when it runs short of eligible ideas.
- **Holdings lag.** Active ETFs publish their holdings daily and the data vendor carries most of them daily, but a day or two behind the fund's own file, and the recalculation runs before that day's file arrives: the positions used are typically one to two trading days old, and up to two weeks old for the funds the vendor refreshes less often.
- **New funds join late.** A fund appears on the active list only after its trust's first N-CEN filing after launch (up to a year), and the list is refreshed monthly.
- **Active ETFs only.** Mutual funds and separately managed accounts, which hold most active assets, are not covered; the manager population is younger and skewed toward sponsors that have launched ETFs, and international managers are fewer than US ones.
- **One firm, several managers.** Funds from the same firm – a global, a US and an international fund drawing on one research team – each count as a manager, so a firm with several qualifying funds has several voices.
- **Vendor data.** Holdings, prices, share counts and style factors come from one vendor (FMP); the validation in [§5](#5-company-data-and-data-quality) catches known failure modes but cannot rule out every error. Lines that cannot be identified (a small share of weight, mostly emerging-market names) take no part.
- **Style is a single label.** Each US company is wholly value or wholly growth; companies near the boundary can change side when the index providers reconstitute, which changes the benchmark they are measured against and the part of the fund they compete for. The same holds for an international company's market when the index provider reclassifies a country.
- **Frontier markets left out.** An international company of neither market – one the international index funds don't hold, in a country they don't cover – can't be held.
- **No risk model.** Equal weighting is a rule, not an optimisation; no sector or country limits, currency hedging, risk model, transaction costs or liquidity constraints are applied.
- **No performance claim.** This document describes how the fund is constructed; it makes no claim about its future performance.

## 13. Glossary

| Term | Meaning |
|------|---------|
| **Active weight (Δ)** | A company's weight in the ETF minus its weight in its reference benchmark. |
| **Appearances** | Number of managers ranking a company among their best ideas. |
| **Best idea** | One of the top 10 positive active weights of an ETF in a week. |
| **Breakpoint** | The company size at which the market's largest companies reach a given share of total market value. |
| **Company market cap** | The whole company's equity value, all share classes, in USD. |
| **Consensus score** | The sum of the rank points a company earns from every manager ranking it. |
| **Developed / emerging market** | An international company's market in FTSE's classification, the index family behind the international index funds (Korea developed). |
| **Float factor** | The investable share of a company's equity, from the index funds' holdings or free float. |
| **Half** | One of the two groups the model portfolio's holdings are divided into. The halves are reviewed on alternate weeks, so each holding is reviewed every two weeks. |
| **Keep buffer** | How far below its part's cut-off a holding may fall and still be kept, as a multiple of the part's target. At 1.0, the fund's setting, a holding is kept only while it is within its part's top list. |
| **Market split** | The fund's division of its holdings between the US and international, of the US holdings between growth and value, and of the international holdings between developed and emerging markets, by the reference benchmarks' float-adjusted values, recalculated quarterly. |
| **Part** | US growth, US value, international developed or international emerging: a share of the fund's holdings, filled from its own ordered list of ideas. A global manager's US and international holdings are also measured as two parts, one per region. |
| **Primary listing** | The listing that represents a company: its home-market ordinary shares. |
| **Rank** | A company's position in a manager's best-ideas list (1 = largest overweight). |
| **Rebalancing** | Trading by a fund that follows the model to keep its positions close to their target weights ([§10](#10-maintaining-the-model-portfolio)). The model portfolio itself doesn't rebalance. |
| **Recalculation** | The weekly review of the model portfolio: its sells, buys and target weights, from the latest best ideas. |
| **Region** | US or international: the index providers' country assignment, else the head-office country, else the country of incorporation, else the listing exchange. |
| **Source ETF** | The ETF with the largest active weight in a fund holding. |
| **Target weight** | A holding's weight in the model portfolio, set at each recalculation: 2% for every holding. |

---

## Appendix A – Parameter reference

### Manager selection

| Parameter | Value |
|-----------|-------|
| SEC active list | Last filing within 15 months |
| Equity fund | At least 75% of holdings in stocks |
| Region | ≥ 80% US (US) / ≤ 20% US (international) / in between (global, measured per region) |
| Size | Large or all-cap: ≥ 70% large stocks for large, by the 70% (large) / 90% (small) size lines |
| Index name | No index provider's brand or "index" in the fund's name (the fund company's name aside) |
| Holdings transparency | Excluded when a prospectus of its own from the last 15 months carries the non-transparent ETF legend |
| Number of stocks | 20–200 (US) / 20–250 (international), a depositary receipt counting as its company's stock; global: both, per region |
| Derivatives | Excluded for good once holdings show an option or an equity-linked note |
| Sector concentration | No sector > 20 points above its highest benchmark weight |
| Top country | ≤ 50% of an international fund, or of a global fund's international holdings |
| Benchmark coverage | ≥ 70% to enter / ≥ 65% to stay (global: in each region) |
| Equal weighting | Excluded when the fund says it weights equally and its 90th-percentile position is under 2× its 10th-percentile one |
| Holdings freshness | Latest holdings ≤ 14 days old |

### Company data and large cap

| Parameter | Value |
|-----------|-------|
| Company region: held by both index markets | One market's float-adjusted holding ≥ 2× the other's decides |
| Market cap validation | 21 days; excluded after 5 days without a good value |
| Large cap: US / international market coverage | 93% / 80% |
| Large cap: minimum float | 10% |

### Style and market

| Parameter | Value |
|-----------|-------|
| US company style from active managers (step 2) | Held by managers with ≥ 60% of their style-classified holdings in one style |
| Market: held by both international index funds | One fund's float-adjusted holding ≥ 2× the other's decides |

### Best ideas

| Parameter | Value |
|-----------|-------|
| Minimum priced holdings | 90% |
| Market cap window | ± 5 days of the holdings date |
| Maximum active weight | 20% |
| Best ideas per ETF | 10 |

### Model portfolio

| Parameter | Value |
|-----------|-------|
| Holdings | 50 |
| US / international | By the market split: the US and international large-cap markets' float-adjusted totals, recalculated quarterly (65.0% / 35.0% → 33 / 17, November 2026) |
| US growth / value | By the US growth and value benchmarks' float-adjusted totals, recalculated quarterly (57.5% / 42.5% → 19 / 14, November 2026); international not split by style |
| International developed / emerging | By the international developed and emerging benchmarks' float-adjusted totals, recalculated quarterly (73.5% / 26.5% → 13 / 4, November 2026) |
| Rank points, ranks 1–10 | 512, 256, 128, 64, 32, 16, 8, 4, 2, 1 |
| Target weight | Equal (2%) |
| Recalculation | Weekly, staggered: two halves reviewed on alternate weeks, each holding every two weeks |
| Keep buffer | 1.0 – each part's top list by score (top 19 US growth / 14 US value / 13 international developed / 4 international emerging at the November 2026 targets) |
| Weekly run | Wednesday on Tuesday's close (moved around NYSE holidays) |

### Rebalancing

| Parameter | Value |
|-----------|-------|
| Position limits | 1.5–2.5% of net assets, checked every two weeks; outside them traded back to 2% |
| Full realignment | Quarterly, every position back to 2% |
| Minimum order | 0.02% of net assets |
