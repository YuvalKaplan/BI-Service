-- Migration 26: staggered rebalancing (by decision, Nov 2026 - the rebalancing study,
-- .output/rebalancing_study/). A staggered fund (strategy.staggered) splits its holdings into two
-- halves reviewed on alternate runs; fund_holding.half records each holding's half (0 / 1), NULL for
-- funds that are not staggered. Appended as the last column (no shift needed).
-- Apply to dev, then prod; delete this file once applied to both.

BEGIN;

ALTER TABLE public.fund_holding ADD COLUMN half smallint;
ALTER TABLE public.fund_holding ADD CONSTRAINT fund_holding_half_check CHECK (half IN (0, 1));

COMMIT;
