# Strategy configuration – Global, Large Cap, Equal Weight, 50 Holdings

The configuration of a strategy-specific methodology document. Claude generates **[global_equal_50.md](global_equal_50.md)** from this file and **[detailed.md](detailed.md)**, and regenerates it whenever either changes. Edit the values below, then ask for a regeneration.

## Document

| Setting | Value |
|---------|-------|
| Title | Global Large Cap Best Ideas – Region and Style by Market Weight, Equal Weight, 50 Holdings |
| Output | `global_equal_50.md` |
| Audience | Investment professionals evaluating or overseeing this fund strategy |
| Fund | "Global, Value / Growth Split Benchmark, Equally Weighted, Large Cap" (the `fund` row, id 11) |

## The fund strategy

| Setting | Value |
|---------|-------|
| Region | Market split by number of holdings: US / International by the float-adjusted totals of the US and International large-cap benchmarks, recalculated quarterly (November 2026: 65.0% / 35.0% → 33 / 17) |
| Size | Large – each region's own large-cap line |
| Style | Market split of the US holdings by number: growth / value by the float-adjusted totals of the US large growth and value benchmarks, recalculated quarterly with the region split (November 2026: 57.5% / 42.5% → 19 / 14 of the 33 US). International holdings: no style split |
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
- other regions, sizes, styles, weightings and splits the fund could have been configured with (fixed splits such as 70/30 or 60/40 growth / value included);
- market-cap weighting (square root, 10% cap, 1% floor);
- the ESG screen;
- ETF / exchange restrictions;
- operational fallbacks that don't change the method (e.g. the blend used when a style snapshot is missing).

## Notes for the writer

- Present the two references as the fund's design: US companies against their style peers, International companies against the whole international large-cap market (no reliable international style classification) – not as options among several. The benchmark a company is measured against follows the company's own style; the style split decides how many US positions each style gets. Explain both and how they fit: the ideas and the allocation rest on the same value / growth benchmarks.
- Foreign companies held by a region's managers (a US manager's TSMC) still count in the manager's total but are never ideas for that manager: say so.
- Explain the market split: measured on the same benchmarks the ideas are measured against (consistency), float-adjusted (why: whole caps would count International's state / family / cross holdings in full – 58% vs 65% US in November 2026), recalculated quarterly, each rebalancing using the latest; the US share of the 50 holdings rounded half up. Quote November 2026 as the illustration (65.0% / 35.0% → 33 / 17), not as a fixed parameter. Don't mention MSCI ACWI.
- Explain the style split the same way: within the US, growth / value by the float-adjusted totals of the US growth and value benchmarks (the same benchmarks the US ideas are measured against), recalculated quarterly with the region split; the growth share of the 33 US holdings rounded half up, value taking the rest (November 2026: 57.5% / 42.5% → 19 / 14). International holdings have no style split (no international style benchmark). A US company of neither style (rare) can't be held.
- Explain that each part – US growth, US value, International – is scored, ordered and kept on its own (keep buffer: top 24 US growth / 18 US value / 22 International at 19 / 14 / 17, moving with the targets), and free positions are filled part by part (a sold US value holding is replaced by a US value idea), so the fund stays at its targets; a part short of eligible ideas is not made up from another.
- When the quarterly split changes the targets, no holding is sold to rebalance: the part above its new target buys nothing until its own sells bring it down, and freed positions go to the part below its target – say so. A company that changes style moves to the other style's part (and is measured against the other benchmark).
- Keep the structure, tone and parameter appendix of `detailed.md`, limited to what applies.
- Selection rules: an outline only – each rule with its level and a one-line reason, and the coverage band; leave out how the rules are measured and how the levels were set (`detailed.md` §4, "How the selection rules are measured and set").
- Never state the number of companies or the number of ETFs (managers, sponsors, funds on the SEC list, benchmark members, companies above a breakpoint). Rules and thresholds stay (e.g. 20–200 stocks per manager, the fund's 50 holdings), and so do dollar breakpoints.
- Explain calculations in plain language, with a worked example where it helps – no formulas or code blocks.
