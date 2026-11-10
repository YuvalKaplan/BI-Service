# Strategy configuration – Global 70/30, Large Cap, Equal Weight, 50 Holdings

The configuration of a strategy-specific methodology document. Claude generates **[global_70_30_equal_50.md](global_70_30_equal_50.md)** from this file and **[detailed.md](detailed.md)**, and regenerates it whenever either changes. Edit the values below, then ask for a regeneration.

## Document

| Setting | Value |
|---------|-------|
| Title | Global Large Cap Best Ideas – 70% US / 30% International, Equal Weight, 50 Holdings |
| Output | `global_70_30_equal_50.md` |
| Audience | Investment professionals evaluating or overseeing this fund strategy |
| Fund | "Global 70/30, Value / Growth Split Benchmark, Equally Weighted, Large Cap" (the `fund` row) |

## The fund strategy

| Setting | Value |
|---------|-------|
| Region | 70% US / 30% International by number of holdings (35 / 15) |
| Size | Large – each region's own large-cap line |
| Style | Any – no style filter |
| Holdings | 50 |
| Weighting | Equal |
| Best ideas measured against | US companies: their own style's US large-cap benchmark (value / growth). International companies: the International large-cap benchmark |
| Rank depth | 1–3 (each manager's top three best ideas) |
| Rebalancing | Every two weeks |
| ESG | Not applied |
| ETF restrictions | None – all active US and International ETFs |
| Exchange restrictions | None |

## Leave out

Anything this strategy doesn't use, so the reader isn't left wondering which option applies:

- the other best-ideas references (self, full universe for the US) and the existence of a choice between references;
- other regions, sizes, styles, weightings and splits the fund could have been configured with;
- market-cap weighting (square root, 10% cap, 1% floor) and its rank buffer of 5;
- the ESG screen;
- ETF / exchange restrictions;
- operational fallbacks that don't change the method (e.g. the blend used when a style snapshot is missing).

## Notes for the writer

- Present the two references as the fund's design: US companies against their style peers, International companies against the whole international large-cap market (no reliable international style classification) – not as options among several.
- Foreign companies held by a region's managers (a US manager's TSMC) still count in the manager's total but are never ideas for that manager: say so.
- Explain that free positions are filled region by region (a sold international holding is replaced by an international idea); holdings are never sold just to restore 70/30, so tolerated holdings can keep a region a position or two above its target, and a region short of eligible ideas is not made up from the other.
- Keep the structure, tone and parameter appendix of `detailed.md`, limited to what applies.
- Never state the number of companies or the number of ETFs (managers, sponsors, funds on the SEC list, benchmark members, companies above a breakpoint). Rules and thresholds stay (e.g. 20–200 stocks per manager, the fund's 50 holdings), and so do dollar breakpoints.
- Explain calculations in plain language, with a worked example where it helps – no formulas or code blocks.
