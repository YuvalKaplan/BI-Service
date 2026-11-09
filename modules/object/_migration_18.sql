-- Migration 18: the US value / growth split of the full_universe benchmark.
-- Two benchmark rows matching US Large Cap Blend (region, cap_type, market_coverage) with a style:
-- the benchmark generator forms each from the blend's companies of that style
-- (benchmark_generator.select_holdings), and best_ideas_generator.get_full_universe_weights
-- measures each company of a US ETF against its own style's benchmark (benchmark.fetch_style_split).
-- International has no split (no international style reference ETFs) and keeps its blend only.
-- No schema change. Apply to dev, then prod; delete this file once applied to both.

INSERT INTO public.benchmark (name, region, cap_type, style_type, market_coverage, disabled)
SELECT v.name, b.region, b.cap_type, v.style_type, b.market_coverage, false
FROM public.benchmark b
CROSS JOIN (VALUES ('US Large Cap Value', 'value'), ('US Large Cap Growth', 'growth')) AS v(name, style_type)
WHERE b.region = 'US' AND b.cap_type = 'large' AND b.style_type = 'blend' AND b.disabled = false
  AND NOT EXISTS (
      SELECT 1 FROM public.benchmark s
      WHERE s.region = b.region AND s.cap_type = b.cap_type AND s.market_coverage = b.market_coverage
        AND s.style_type = v.style_type
  );
