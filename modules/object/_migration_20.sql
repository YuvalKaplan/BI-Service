-- Migration 20: funds select by consensus score (model_fund.Strategy.rank_points: each ETF gives a
-- company 512 / 256 / 128 / ... / 1 points for its rank 1..10 there, summed across the ETFs).
-- fund_holding and fund_holding_change get a `score` column right after `ranking` (the
-- x_d_shift_columns_template pattern), and the funds' strategies lose ranking_from / ranking_to,
-- which the selection no longer reads. Apply to dev, then prod; delete this file once applied to both.

BEGIN;

-- fund_holding: ranking, [score], source_etf_id, max_delta, weight
ALTER TABLE public.fund_holding ADD COLUMN score integer;
ALTER TABLE public.fund_holding ADD COLUMN _source_etf_id integer;
ALTER TABLE public.fund_holding ADD COLUMN _max_delta double precision;
ALTER TABLE public.fund_holding ADD COLUMN _weight double precision;
UPDATE public.fund_holding SET _source_etf_id = source_etf_id, _max_delta = max_delta, _weight = weight;
ALTER TABLE public.fund_holding DROP COLUMN source_etf_id;
ALTER TABLE public.fund_holding DROP COLUMN max_delta;
ALTER TABLE public.fund_holding DROP COLUMN weight;
ALTER TABLE public.fund_holding RENAME COLUMN _source_etf_id TO source_etf_id;
ALTER TABLE public.fund_holding RENAME COLUMN _max_delta TO max_delta;
ALTER TABLE public.fund_holding RENAME COLUMN _weight TO weight;

-- fund_holding_change: ranking, [score], appearances, max_delta, top_delta_provider_etf_id,
-- all_provider_etf_ids, reason
ALTER TABLE public.fund_holding_change ADD COLUMN score integer;
ALTER TABLE public.fund_holding_change ADD COLUMN _appearances integer;
ALTER TABLE public.fund_holding_change ADD COLUMN _max_delta double precision;
ALTER TABLE public.fund_holding_change ADD COLUMN _top_delta_provider_etf_id integer;
ALTER TABLE public.fund_holding_change ADD COLUMN _all_provider_etf_ids integer[];
ALTER TABLE public.fund_holding_change ADD COLUMN _reason text;
UPDATE public.fund_holding_change SET
    _appearances = appearances,
    _max_delta = max_delta,
    _top_delta_provider_etf_id = top_delta_provider_etf_id,
    _all_provider_etf_ids = all_provider_etf_ids,
    _reason = reason;
ALTER TABLE public.fund_holding_change DROP COLUMN appearances;
ALTER TABLE public.fund_holding_change DROP COLUMN max_delta;
ALTER TABLE public.fund_holding_change DROP COLUMN top_delta_provider_etf_id;
ALTER TABLE public.fund_holding_change DROP COLUMN all_provider_etf_ids;
ALTER TABLE public.fund_holding_change DROP COLUMN reason;
ALTER TABLE public.fund_holding_change RENAME COLUMN _appearances TO appearances;
ALTER TABLE public.fund_holding_change RENAME COLUMN _max_delta TO max_delta;
ALTER TABLE public.fund_holding_change RENAME COLUMN _top_delta_provider_etf_id TO top_delta_provider_etf_id;
ALTER TABLE public.fund_holding_change RENAME COLUMN _all_provider_etf_ids TO all_provider_etf_ids;
ALTER TABLE public.fund_holding_change RENAME COLUMN _reason TO reason;

-- The strategies: the score replaces ranking_from / ranking_to (rank_points has a default)
UPDATE public.fund SET strategy = strategy - 'ranking_from' - 'ranking_to'
WHERE strategy ?| array['ranking_from', 'ranking_to'];

COMMIT;
