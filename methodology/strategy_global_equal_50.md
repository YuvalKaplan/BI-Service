# Strategy configuration – Global, Large Cap, Equal Weight, 50 Holdings

The configuration of a strategy-specific methodology document. Claude generates **[global_equal_50.md](global_equal_50.md)** from this file and **[detailed.md](detailed.md)**, and regenerates it whenever either changes. Edit the values below, then ask for a regeneration.

## Document

| Setting | Value |
|---------|-------|
| Title | Global Large Cap Best Ideas |
| Output | `global_equal_50.md` |
| Audience | Investment professionals evaluating or overseeing this fund strategy |
| Fund | "Global, Value / Growth Split Benchmark, Equally Weighted, Large Cap" (the `fund` row, id 11) |

## The fund strategy

| Setting | Value |
|---------|-------|
| Region | Market split by number of holdings: US / International by the float-adjusted totals of the US and International large-cap benchmarks, recalculated quarterly (November 2026: 65.0% / 35.0% → 33 / 17) |
| Size | Large – each region's own large-cap line |
| Style | Market split of the US holdings by number: growth / value by the float-adjusted totals of the US large growth and value benchmarks, recalculated quarterly with the region split (November 2026: 57.5% / 42.5% → 19 / 14 of the 33 US). International holdings: no style split |
| International market | Market split of the International holdings by number: developed / emerging by the float-adjusted totals of the International large developed and emerging benchmarks, recalculated quarterly with the region split (November 2026: 73.5% / 26.5% → 13 / 4 of the 17 International) |
| Holdings | 50 |
| Weighting | Equal |
| Best ideas measured against | US companies: their own style's US large-cap benchmark (value / growth). International companies: their own market's International large-cap benchmark (developed / emerging) |
| Rank points | 512, 256, 128, 64, 32, 16, 8, 4, 2, 1 for ranks 1–10 (the default) – every manager's ten best ideas count, each rank half the one above |
| Recalculation | Weekly, staggered: the holdings in two halves, one half reviewed every week, so each holding is reviewed every two weeks |
| Keep buffer | 1.0 – each part holds exactly its current top list; a holding that falls below its part's cut-off is sold at its next recalculation |
| ESG | Not applied |
| ETF restrictions | None – all active US, International and Global ETFs (a Global ETF's US part feeds the US holdings, its International part the International holdings) |
| Exchange restrictions | None |

## Leave out

Anything this strategy doesn't use, so the reader isn't left wondering which option applies:

- the other best-ideas references (self, full universe for the US) and the existence of a choice between references;
- other regions, sizes, styles, weightings and splits the fund could have been configured with (fixed splits such as 70/30 or 60/40 growth / value included);
- market-cap weighting (square root, 10% cap, 1% floor);
- the ESG screen;
- ETF / exchange restrictions;
- operational fallbacks that don't change the method (e.g. the blend used when a style or market snapshot is missing);
- the former emerging-markets exclusion and the former 35% country limit (history, not this fund's method).

## Notes for the writer

- Present the two references as the fund's design: US companies against their style peers, International companies against their market peers – developed or emerging, FTSE's classification (no reliable international style classification, but every company's market is known) – not as options among several. The benchmark a company is measured against follows the company's own style or market; the style and market splits decide how many US positions each style gets and how many International positions each market gets. Explain both and how they fit: the ideas and the allocation rest on the same benchmarks. Explain why emerging-markets managers are in the universe: measured against their own market's benchmark, their overweights are stock picks, not the developed / emerging allocation.
- Foreign companies held by a region's managers (a US manager's TSMC) still count in the manager's total but are never ideas for that manager: say so.
- Global managers (between 20% and 80% US) are measured as two managers, one per region: explain the parts (re-weighted within the region, measured like a fund of that region, own top 10, scored in that region's list), the three skews one combined measure would bring (region allocation leaking into stock ideas, the style benchmarks favouring International companies, ranks not comparing), and the selection rules applied per part (20 stocks in each region, country / coverage on the International part or each part).
- Explain the market split: measured on the same benchmarks the ideas are measured against (consistency), float-adjusted (why: whole caps would count International's state / family / cross holdings in full – 58% vs 65% US in November 2026), recalculated quarterly, each recalculation using the latest; the US share of the 50 holdings rounded half up. Quote November 2026 as the illustration (65.0% / 35.0% → 33 / 17), not as a fixed parameter. Don't mention MSCI ACWI.
- Explain the style split the same way: within the US, growth / value by the float-adjusted totals of the US growth and value benchmarks (the same benchmarks the US ideas are measured against), recalculated quarterly with the region split; the growth share of the 33 US holdings rounded half up, value taking the rest (November 2026: 57.5% / 42.5% → 19 / 14). International holdings have no style split (no international style benchmark). A US company of neither style (rare) can't be held.
- Explain the International market split the same way: developed / emerging by the float-adjusted totals of the International developed and emerging benchmarks (the same benchmarks the International ideas are measured against), recalculated quarterly with the region split; the emerging share of the 17 International holdings rounded half up, developed taking the rest (November 2026: 73.5% / 26.5% → 13 / 4). An International company of neither market (frontier markets) can't be held. Quote it as an illustration, not a parameter.
- Explain that each part – US growth, US value, International developed, International emerging – is scored, ordered and kept on its own, and free positions are filled part by part (a sold US value holding is replaced by a US value idea), so the fund stays at its targets; a part short of eligible ideas is not made up from another – except that an emerging part short of ideas leaves its places to the developed part.
- The keep buffer is 1.0: the keep line is each part's cut-off (19 / 14 / 13 / 4 at the November 2026 targets, moving with them), so the fund holds exactly each part's top list and an idea that rises into it replaces the part's lowest holding at the next recalculation. Don't describe a tolerance band or a "price of lower turnover". Say briefly why 1.0 (a wider buffer keeps names whose consensus has weakened; the fund follows the current consensus) and that the choice is reviewed as history accumulates.
- Explain the staggered recalculation: two halves, alternating weeks, each holding reviewed every two weeks on its half's week; a holding of the other half that slips below its part's cut-off waits for its own week (the lowest-scored of that week's buys in its part is deferred); one no longer among any manager's best ideas is sold at once. Why: it limits the turnover of a 1.0 buffer – a holding that slips below its cut-off gets until its own half's week to recover – while new ideas still enter every week, and trades are spread over every week. Reviewed as history accumulates.
- When the quarterly split changes the targets, the keep lines move with them: a part now above its target sells its lowest-scored holdings beyond the new target as each half's week comes, and the freed positions go to the part below its target, so the new split is reached within two weeks – say so. A company that changes style moves to the other style's part (and is measured against the other benchmark); likewise a company the index provider reclassifies between developed and emerging.
- Keep the structure, tone and parameter appendix of `detailed.md`, limited to what applies.
- The opening paragraph under the title: short, plain sentences; name all three splits (US / international, US growth / value, international developed / emerging).
- §1 "Background": the idea in a few sentences (no numbered list of implementation steps – those are the summary tables and the later sections), then a "Strategy" subsection with the summary tables.
- One section on each company's peers – a US company's style and an international company's developed / emerging market, side by side, with why the US is grouped by style and international by market and what happens to a company of neither. The portfolio section has one "market split" subsection covering all three splits together (one table of the November 2026 figures), not one subsection per split.
- §3 is headed "The managers" (no subtitle). The peers section (§7) has no separate "companies of neither" subsection: a company with no style or market is mentioned where it matters (measured against its whole region; can't be held).
- §10 "Maintaining the model portfolio" opens by saying it describes the model portfolio (the list of holdings and target weights; its buys and sells are list changes, not trades; how a fund following it trades is outside the method) and says "model portfolio", not "fund", throughout. It follows a week's run in order: the two halves; the weekly run as numbered steps (sell no-longer-best-ideas at once; sell the reviewed half's holdings that dropped out of their part's top list, the other half's waiting for their week; buy up to each part's target, a waiting holding still taking a place; set every holding's target weight to 1/50); a worked example; why (keep buffer 1.0, the halves); split / classification changes; other rules. Short plain sentences.
- §10 ends with a "Rebalancing" subsection: the rules for a fund that follows the model (name changes each week for the reviewed half, at 2% of net assets in the same part, a no-longer-best-idea sold at once, leftover cash to the positions furthest below 2% / shortfall from those furthest above; every two weeks positions outside 1.5–2.5% traded back to 2%; deposits to positions below 2%, withdrawals from those above, largest gap first; quarterly full realignment to 2% with the new split; orders below 0.02% of net assets skipped). The §10 opening points to it. No study figures.
- Terms: *recalculation* is the weekly review of the holdings (best ideas → buys and sells); the model portfolio only states target weights (1/50 each, set at every recalculation) – it does not rebalance (no price drift is tracked or traded back). *Rebalancing* is only what a fund following the model does (the Rebalancing subsection). Never call a recalculation a rebalance.
- Never cite the March–October 2026 replay / rebalancing study (or any backtest on that short history) as evidence; give the reasoning instead.
- Selection rules: an outline only – each rule with its level and a one-line reason, and the coverage band; leave out how the rules are measured and how the levels were set (`detailed.md` §3, "How the selection rules are measured and set").
- Never state the number of companies or the number of ETFs (managers, sponsors, funds on the SEC list, benchmark members, companies above a breakpoint). Rules and thresholds stay (e.g. 20–200 stocks per manager, the fund's 50 holdings), and so do dollar breakpoints.
- Explain calculations in plain language, with a worked example where it helps – no formulas or code blocks.
