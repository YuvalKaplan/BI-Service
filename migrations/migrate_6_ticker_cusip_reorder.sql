-- ============================================================
-- Phase 6: Add cusip column to ticker and reorder columns.
--
-- Current order:
--   id, symbol, created_at, source, style_type, cap_type,
--   type_from, isin, cik, exchange, name, industry, sector,
--   currency, esg_factors, esg_qualified, invalid
--
-- Target order:
--   id, created_at, source, style_type, cap_type, type_from,
--   symbol, isin, cusip, cik, exchange, name, industry, sector,
--   currency, esg_factors, esg_qualified, invalid
--
-- Uses the x_d_shift_columns pattern: add _ copies in target
-- order, copy data, drop originals, rename.
-- ============================================================

ALTER TABLE public.ticker
    ADD COLUMN _created_at    timestamp without time zone,
    ADD COLUMN _source        character varying(16),
    ADD COLUMN _style_type    character varying(16),
    ADD COLUMN _cap_type      character varying(16),
    ADD COLUMN _type_from     character varying(16),
    ADD COLUMN _symbol        character varying(32),
    ADD COLUMN _isin          character varying(16),
    ADD COLUMN _cusip         character varying(16),
    ADD COLUMN _cik           character varying(16),
    ADD COLUMN _exchange      character varying(32),
    ADD COLUMN _name          text,
    ADD COLUMN _industry      text,
    ADD COLUMN _sector        text,
    ADD COLUMN _currency      character varying(8),
    ADD COLUMN _esg_factors   jsonb,
    ADD COLUMN _esg_qualified boolean,
    ADD COLUMN _invalid       text;

UPDATE public.ticker SET
    _created_at    = created_at,
    _source        = source,
    _style_type    = style_type,
    _cap_type      = cap_type,
    _type_from     = type_from,
    _symbol        = symbol,
    _isin          = isin,
    _cusip         = NULL,
    _cik           = cik,
    _exchange      = exchange,
    _name          = name,
    _industry      = industry,
    _sector        = sector,
    _currency      = currency,
    _esg_factors   = esg_factors,
    _esg_qualified = esg_qualified,
    _invalid       = invalid;

ALTER TABLE public.ticker
    DROP CONSTRAINT ticker_symbol_key,
    DROP CONSTRAINT ticker_isin_key;

ALTER TABLE public.ticker
    DROP COLUMN symbol,
    DROP COLUMN created_at,
    DROP COLUMN source,
    DROP COLUMN style_type,
    DROP COLUMN cap_type,
    DROP COLUMN type_from,
    DROP COLUMN isin,
    DROP COLUMN cik,
    DROP COLUMN exchange,
    DROP COLUMN name,
    DROP COLUMN industry,
    DROP COLUMN sector,
    DROP COLUMN currency,
    DROP COLUMN esg_factors,
    DROP COLUMN esg_qualified,
    DROP COLUMN invalid;

ALTER TABLE public.ticker RENAME COLUMN _created_at    TO created_at;
ALTER TABLE public.ticker RENAME COLUMN _source        TO source;
ALTER TABLE public.ticker RENAME COLUMN _style_type    TO style_type;
ALTER TABLE public.ticker RENAME COLUMN _cap_type      TO cap_type;
ALTER TABLE public.ticker RENAME COLUMN _type_from     TO type_from;
ALTER TABLE public.ticker RENAME COLUMN _symbol        TO symbol;
ALTER TABLE public.ticker RENAME COLUMN _isin          TO isin;
ALTER TABLE public.ticker RENAME COLUMN _cusip         TO cusip;
ALTER TABLE public.ticker RENAME COLUMN _cik           TO cik;
ALTER TABLE public.ticker RENAME COLUMN _exchange      TO exchange;
ALTER TABLE public.ticker RENAME COLUMN _name          TO name;
ALTER TABLE public.ticker RENAME COLUMN _industry      TO industry;
ALTER TABLE public.ticker RENAME COLUMN _sector        TO sector;
ALTER TABLE public.ticker RENAME COLUMN _currency      TO currency;
ALTER TABLE public.ticker RENAME COLUMN _esg_factors   TO esg_factors;
ALTER TABLE public.ticker RENAME COLUMN _esg_qualified TO esg_qualified;
ALTER TABLE public.ticker RENAME COLUMN _invalid       TO invalid;

ALTER TABLE public.ticker
    ALTER COLUMN symbol     SET NOT NULL,
    ALTER COLUMN created_at SET NOT NULL,
    ALTER COLUMN created_at SET DEFAULT (now() AT TIME ZONE 'utc'::text);

ALTER TABLE public.ticker ADD CONSTRAINT ticker_symbol_key UNIQUE (symbol);
ALTER TABLE public.ticker ADD CONSTRAINT ticker_isin_key   UNIQUE (isin);

-- After: id, created_at, source, style_type, cap_type, type_from,
--        symbol, isin, cusip, cik, exchange, name, industry, sector,
--        currency, esg_factors, esg_qualified, invalid
