-- Migration 23: Global ETFs feed best ideas as two parts (best_ideas_generator.parts_of): their US
-- companies and their International ones, each measured and ranked as an ETF of that region.
-- best_idea gets `part` after benchmark_mode, fund_analysis gets `part` right after provider_etf_id
-- (the x_d_shift_columns_template pattern): 'US' / 'International' for a Global ETF's rows, NULL for
-- a regional ETF's. Existing rows are all regional ETFs' (Global ETFs were never active): NULL.
-- Apply to dev, then prod; delete this file once applied to both.

BEGIN;

-- best_idea: ..., benchmark_mode, [part]
ALTER TABLE public.best_idea ADD COLUMN part text;

-- fund_analysis: fund_id, as_of_date, provider_etf_id, [part], holding_date, ticker_id, benchmark_id,
-- benchmark_date, market_cap, master_used, etf_weight, benchmark_weight, delta, ranking, note
ALTER TABLE public.fund_analysis ADD COLUMN part text;
ALTER TABLE public.fund_analysis ADD COLUMN _holding_date date;
ALTER TABLE public.fund_analysis ADD COLUMN _ticker_id integer;
ALTER TABLE public.fund_analysis ADD COLUMN _benchmark_id integer;
ALTER TABLE public.fund_analysis ADD COLUMN _benchmark_date date;
ALTER TABLE public.fund_analysis ADD COLUMN _market_cap double precision;
ALTER TABLE public.fund_analysis ADD COLUMN _master_used boolean DEFAULT false;
ALTER TABLE public.fund_analysis ADD COLUMN _etf_weight double precision;
ALTER TABLE public.fund_analysis ADD COLUMN _benchmark_weight double precision;
ALTER TABLE public.fund_analysis ADD COLUMN _delta double precision;
ALTER TABLE public.fund_analysis ADD COLUMN _ranking integer;
ALTER TABLE public.fund_analysis ADD COLUMN _note text;
UPDATE public.fund_analysis SET
    _holding_date = holding_date,
    _ticker_id = ticker_id,
    _benchmark_id = benchmark_id,
    _benchmark_date = benchmark_date,
    _market_cap = market_cap,
    _master_used = master_used,
    _etf_weight = etf_weight,
    _benchmark_weight = benchmark_weight,
    _delta = delta,
    _ranking = ranking,
    _note = note;
-- ticker_id is in the primary key and both id columns carry foreign keys: drop them first
ALTER TABLE public.fund_analysis DROP CONSTRAINT fund_analysis_pkey;
ALTER TABLE public.fund_analysis DROP CONSTRAINT fk_fund_analysis_ticker_id;
ALTER TABLE public.fund_analysis DROP CONSTRAINT fk_fund_analysis_benchmark_id;
ALTER TABLE public.fund_analysis DROP COLUMN holding_date;
ALTER TABLE public.fund_analysis DROP COLUMN ticker_id;
ALTER TABLE public.fund_analysis DROP COLUMN benchmark_id;
ALTER TABLE public.fund_analysis DROP COLUMN benchmark_date;
ALTER TABLE public.fund_analysis DROP COLUMN market_cap;
ALTER TABLE public.fund_analysis DROP COLUMN master_used;
ALTER TABLE public.fund_analysis DROP COLUMN etf_weight;
ALTER TABLE public.fund_analysis DROP COLUMN benchmark_weight;
ALTER TABLE public.fund_analysis DROP COLUMN delta;
ALTER TABLE public.fund_analysis DROP COLUMN ranking;
ALTER TABLE public.fund_analysis DROP COLUMN note;
ALTER TABLE public.fund_analysis RENAME COLUMN _holding_date TO holding_date;
ALTER TABLE public.fund_analysis RENAME COLUMN _ticker_id TO ticker_id;
ALTER TABLE public.fund_analysis RENAME COLUMN _benchmark_id TO benchmark_id;
ALTER TABLE public.fund_analysis RENAME COLUMN _benchmark_date TO benchmark_date;
ALTER TABLE public.fund_analysis RENAME COLUMN _market_cap TO market_cap;
ALTER TABLE public.fund_analysis RENAME COLUMN _master_used TO master_used;
ALTER TABLE public.fund_analysis RENAME COLUMN _etf_weight TO etf_weight;
ALTER TABLE public.fund_analysis RENAME COLUMN _benchmark_weight TO benchmark_weight;
ALTER TABLE public.fund_analysis RENAME COLUMN _delta TO delta;
ALTER TABLE public.fund_analysis RENAME COLUMN _ranking TO ranking;
ALTER TABLE public.fund_analysis RENAME COLUMN _note TO note;
ALTER TABLE public.fund_analysis ALTER COLUMN holding_date SET NOT NULL;
ALTER TABLE public.fund_analysis ALTER COLUMN ticker_id SET NOT NULL;
ALTER TABLE public.fund_analysis ALTER COLUMN master_used SET NOT NULL;
ALTER TABLE public.fund_analysis ALTER COLUMN note SET NOT NULL;
ALTER TABLE public.fund_analysis ADD CONSTRAINT fund_analysis_pkey PRIMARY KEY (fund_id, as_of_date, provider_etf_id, ticker_id);
ALTER TABLE public.fund_analysis ADD CONSTRAINT fk_fund_analysis_ticker_id FOREIGN KEY (ticker_id) REFERENCES public.ticker(id) ON UPDATE CASCADE ON DELETE CASCADE;
ALTER TABLE public.fund_analysis ADD CONSTRAINT fk_fund_analysis_benchmark_id FOREIGN KEY (benchmark_id) REFERENCES public.benchmark(id) ON UPDATE CASCADE ON DELETE SET NULL;

COMMIT;
