# Strategy configuration – Global, Large Cap, Equal Weight, 50 Holdings

The configuration of a strategy-specific methodology document. Claude generates **[global_equal_50.md](global_equal_50.md)** from this file and **[detailed.md](detailed.md)**, and regenerates it whenever either changes. Edit the values below, then ask for a regeneration.

## Document

| Setting | Value |
|---------|-------|
| Title | Global Large Cap Best Ideas – US / International by Market Weight, Equal Weight, 50 Holdings |
| Output | `global_equal_50.md` |
| Audience | Investment professionals evaluating or overseeing this fund strategy |
| Fund | "Global, Value / Growth Split Benchmark, Equally Weighted, Large Cap" (the `fund` row, id 11) |

## The fund strategy

| Setting | Value |
|---------|-------|
| Region | Market split by number of holdings: US / International by the float-adjusted totals of the US and International large-cap benchmarks, recalculated quarterly (November 2026: 65.0% / 35.0% → 33 / 17) |
| Size | Large – each region's own large-cap line |
| Style | Any – no style filter |
| Holdings | 50 |
| Weighting | Equal |
| Best ideas measured against | US companies: their own style's US large-cap benchmark (value / growth). International companies: the International large-cap benchmark |
| Rank points | 512, 256, 128, 64, 32, 16, 8, 4, 2, 1 for ranks 1–10 (the default) – every manager's ten best ideas count, each rank half the one above |
| Rebalancing | Every two weeks |
| ESG | Not applied |
| ETF restrictions | None – all active US and International ETFs |
| Exchange restrictions | None |

## Leave out

Anything this strategy doesn't use, so the reader isn't left wondering which option applies:

- the other best-ideas references (self, full universe for the US) and the existence of a choice between references;
- other regions, sizes, styles, weightings and splits the fund could have been configured with (a fixed split such as 70/30 included);
- market-cap weighting (square root, 10% cap, 1% floor);
- the ESG screen;
- ETF / exchange restrictions;
- operational fallbacks that don't change the method (e.g. the blend used when a style snapshot is missing).

## Notes for the writer

- Present the two references as the fund's design: US companies against their style peers, International companies against the whole international large-cap market (no reliable international style classification) – not as options among several.
- Foreign companies held by a region's managers (a US manager's TSMC) still count in the manager's total but are never ideas for that manager: say so.
- Explain the market split: measured on the same benchmarks the ideas are measured against (consistency), float-adjusted (why: whole caps would count International's state / family / cross holdings in full – 58% vs 65% US in November 2026), recalculated quarterly, each rebalancing using the latest; the US share of the 50 holdings rounded half up. Quote November 2026 as the illustration (65.0% / 35.0% → 33 / 17), not as a fixed parameter. Don't mention the value / growth market split (the fund has no style split) or MSCI ACWI.
- Explain that each region is scored, ordered and kept on its own (keep buffer: top 42 US / 22 International at 33 / 17, moving with the targets), and free positions are filled region by region (a sold international holding is replaced by an international idea), so the fund stays at its targets; a region short of eligible ideas is not made up from the other.
- When the quarterly split changes the targets, no holding is sold to rebalance: the region above its new target buys nothing until its own sells bring it down, and freed positions go to the region below its target – say so.
- Keep the structure, tone and parameter appendix of `detailed.md`, limited to what applies.
- Selection rules: an outline only – each rule with its level and a one-line reason, and the coverage band; leave out how the rules are measured and how the levels were set (`detailed.md` §4, "How the selection rules are measured and set").
- Never state the number of companies or the number of ETFs (managers, sponsors, funds on the SEC list, benchmark members, companies above a breakpoint). Rules and thresholds stay (e.g. 20–200 stocks per manager, the fund's 50 holdings), and so do dollar breakpoints.
- Explain calculations in plain language, with a worked example where it helps – no formulas or code blocks.
