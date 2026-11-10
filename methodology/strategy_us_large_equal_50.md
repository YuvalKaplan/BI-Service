# Strategy configuration – US Large Cap, Equal Weight, 50 Holdings

The configuration of a strategy-specific methodology document. Claude generates **[us_large_equal_50.md](us_large_equal_50.md)** from this file and **[detailed.md](detailed.md)**, and regenerates it whenever either changes. Edit the values below, then ask for a regeneration.

## Document

| Setting | Value |
|---------|-------|
| Title | US Large Cap Best Ideas – Equal Weight, 50 Holdings |
| Output | `us_large_equal_50.md` |
| Audience | Investment professionals evaluating or overseeing this fund strategy |

## The fund strategy

| Setting | Value |
|---------|-------|
| Region | US |
| Size | Large |
| Style | Any – no style filter; value and growth companies compete on rank alone |
| Holdings | 50 |
| Weighting | Equal |
| Best ideas measured against | Value / growth split – each company against its own style's US large-cap benchmark |
| Rank depth | 1–3 (each manager's top three best ideas) |
| Rebalancing | Every two weeks |
| ESG | Not applied |
| ETF restrictions | None – all active US ETFs |
| Exchange restrictions | None |

## Leave out

Anything this strategy doesn't use, so the reader isn't left wondering which option applies:

- the other best-ideas references (self, full universe) and the existence of a choice between references;
- International: international ETFs, benchmarks, breakpoints, emerging-market rules, VEA / VWO;
- other regions, sizes, styles, weightings and splits the fund could have been configured with;
- market-cap weighting (square root, 10% cap, 1% floor) and its rank buffer of 5;
- the ESG screen;
- ETF and exchange restrictions;
- operational fallbacks that don't change the method (e.g. the blend used when a style snapshot is missing).

## Notes for the writer

- Present the value / growth benchmarks as the fund's reference, with the reason it is used – not as one option among several.
- Foreign companies held by US managers (TSMC, Shell) still count in the manager's total but are never ideas: say so, since the reader will see them in the ETFs' holdings.
- Keep the structure, tone and parameter appendix of `detailed.md`, limited to what applies.
- Never state the number of companies or the number of ETFs (managers, sponsors, funds on the SEC list, benchmark members, companies above a breakpoint). Rules and thresholds stay (e.g. 20–200 stocks per manager, the fund's 50 holdings), and so do dollar breakpoints.
- Explain calculations in plain language, with a worked example where it helps – no formulas or code blocks.
