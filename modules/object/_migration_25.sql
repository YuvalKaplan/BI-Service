-- Migration 25: the International developed / emerging split (by decision, Nov 2026 - as the US
-- value / growth split): each International company is measured against the benchmark of its own
-- market, developed or emerging (FTSE's view, from the index funds: VEA developed, VWO emerging).
--
-- - ticker.market_class ('developed' | 'emerging' | NULL), after region: set by
--   master.refresh_company_data (company.market_class), NULL until its first run.
-- - benchmark.market_class (NULL = the whole region), after style_type; two new benchmarks split
--   the International blend: Intl Large Cap Developed / Emerging (its region, cap_type and
--   market_coverage - benchmark.fetch_split), and benchmark_split takes a 'market' split type
--   (developed / emerging, quarterly, for the funds' International market buckets).
-- - The market mode is renamed: best_idea.benchmark_mode 'full_value_growth_split' -> 'market_split'
--   (best_idea.MODE_MARKET; it now covers both splits), and any fund strategy naming it.
-- Apply to dev, then prod; delete this file once applied to both.

BEGIN;

-- ticker: ..., region, [market_class], invalid
ALTER TABLE public.ticker ADD COLUMN market_class text;
ALTER TABLE public.ticker ADD COLUMN _invalid text;
UPDATE public.ticker SET _invalid = invalid;
ALTER TABLE public.ticker DROP COLUMN invalid;
ALTER TABLE public.ticker RENAME COLUMN _invalid TO invalid;

-- benchmark: ..., style_type, [market_class], market_coverage, disabled
ALTER TABLE public.benchmark ADD COLUMN market_class text;
ALTER TABLE public.benchmark ADD COLUMN _market_coverage numeric(4,2);
ALTER TABLE public.benchmark ADD COLUMN _disabled boolean DEFAULT false;
UPDATE public.benchmark SET
    _market_coverage = market_coverage,
    _disabled = disabled;
ALTER TABLE public.benchmark DROP COLUMN market_coverage;
ALTER TABLE public.benchmark DROP COLUMN disabled;
ALTER TABLE public.benchmark RENAME COLUMN _market_coverage TO market_coverage;
ALTER TABLE public.benchmark RENAME COLUMN _disabled TO disabled;
ALTER TABLE public.benchmark ALTER COLUMN market_coverage SET NOT NULL;
ALTER TABLE public.benchmark ALTER COLUMN disabled SET NOT NULL;

INSERT INTO public.benchmark (name, region, cap_type, style_type, market_class, market_coverage, disabled)
SELECT v.name, b.region, b.cap_type, b.style_type, v.market_class, b.market_coverage, false
FROM public.benchmark b
CROSS JOIN (VALUES ('Intl Large Cap Developed', 'developed'), ('Intl Large Cap Emerging', 'emerging')) AS v(name, market_class)
WHERE b.name = 'Intl Large Cap Blend';

-- benchmark_split: a third split type, 'market' (developed / emerging - modules/cron/market_split.py)
ALTER TABLE public.benchmark_split DROP CONSTRAINT benchmark_split_split_type_check;
ALTER TABLE public.benchmark_split ADD CONSTRAINT benchmark_split_split_type_check CHECK (split_type IN ('region', 'style', 'market'));

-- The market mode's new name
UPDATE public.best_idea SET benchmark_mode = 'market_split' WHERE benchmark_mode = 'full_value_growth_split';
UPDATE public.fund SET strategy = jsonb_set(strategy::jsonb, '{benchmark}', '"market_split"')
WHERE strategy->>'benchmark' = 'full_value_growth_split';

COMMIT;
