-- Migration 19: the Global 70/30 fund – 70% US companies measured against their own style's
-- benchmark (US Large Cap Value / Growth), 30% International measured against the International
-- blend (the value / growth reference falls back to the blend where a region has no split);
-- large cap, equal weight, 50 holdings, each manager's top 3 ideas, rebalanced every two weeks,
-- no ESG filter. Methodology: methodology/global_70_30_equal_50.md.
-- No schema change. Apply to dev, then prod; delete this file once applied to both.

INSERT INTO public.fund (name, strategy, active)
SELECT 'Global 70/30, Value / Growth Split Benchmark, Equally Weighted, Large Cap',
       '{
          "cap": {"name": "large"},
          "style": {"name": "core"},
          "region": {"name": "Global", "split": {"US": 70, "International": 30}},
          "holdings": 50,
          "benchmark": "full_value_growth_split",
          "allocation": "equal",
          "esg_only": false,
          "ranking_from": 1,
          "ranking_to": 3,
          "recalc_frequency_days": 14
        }'::jsonb,
       true
WHERE NOT EXISTS (
    SELECT 1 FROM public.fund
    WHERE name = 'Global 70/30, Value / Growth Split Benchmark, Equally Weighted, Large Cap'
);
