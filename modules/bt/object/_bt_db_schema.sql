--
-- PostgreSQL database dump
--

\restrict 2AXA0yBiFgiN8zjZdbEel0fgHpLJFcxfwHZ5Od5G3xQoKbCqTBYiuEza9x90cxf

-- Dumped from database version 17.6
-- Dumped by pg_dump version 18.0

-- Started on 2026-04-11 20:06:36

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: sanitize_tickers(); Type: FUNCTION; Schema: public; Owner: admin
--

CREATE OR REPLACE FUNCTION public.sanitize_tickers()
RETURNS void
LANGUAGE sql
AS $$
    WITH candidates AS (
        SELECT symbol,
               CASE
                   WHEN symbol !~ '^[A-Z]{1,5}$'  THEN 'Invalid ticker format'
                   WHEN name ~* '\m(ETF|fund)\M'   THEN 'Fund or ETF'
                   WHEN symbol = ANY(ARRAY[
                       'XTSLA','AGPXX','BOXX','CMQXX','DTRXX','FGXXX',
                       'FTIXX','GVMXX','JIMXX','JTSXX','MGMXX','PGLXX','SALXX'
                   ])                              THEN 'Treasury Security'
               END AS reason
        FROM public.ticker
        WHERE invalid IS NULL
    )
    UPDATE public.ticker t
    SET invalid = c.reason
    FROM candidates c
    WHERE t.symbol = c.symbol
      AND c.reason IS NOT NULL;
$$;

ALTER FUNCTION public.sanitize_tickers() OWNER TO admin;


--
-- TOC entry 273 (class 1255 OID 250674)
-- Name: get_best_ideas_by_ranking(integer, text, text, date, integer[]); Type: FUNCTION; Schema: public; Owner: admin
--

CREATE FUNCTION public.get_best_ideas_by_ranking(p_ranking_level integer, p_style_type text, p_cap_type text, p_as_of_date date, p_provider_etf_ids integer[]) RETURNS TABLE(symbol text, ranking integer, appearances bigint, max_delta double precision, source_etf_id integer, all_provider_ids integer[])
    LANGUAGE sql
    AS $$

	WITH constants AS (
	    SELECT INTERVAL '10 days' AS lookback
	),
	
	-- 1. Pre-filter the "Universe" of allowed symbols based on Style (and calculate cap level)
	eligible_symbols AS (
	    SELECT DISTINCT ON (t.symbol) 
	        t.symbol,
	        (CASE WHEN tv.market_cap >= 10000000000 THEN 'large' ELSE 'mid_small' END) as calc_cap
	    FROM public.ticker t
	    JOIN public.ticker_value tv ON t.symbol = tv.symbol, 
	    constants
	    WHERE tv.value_date <= p_as_of_date
	      AND tv.value_date >= p_as_of_date - lookback
	      -- Style Filter applied here
	      AND (p_style_type = 'blend' OR t.style_type = p_style_type)
	    ORDER BY t.symbol, tv.value_date DESC
	),
	
	-- 2. Further restrict by the Cap Filter
	filtered_universe AS (
	    SELECT symbol 
	    FROM eligible_symbols
	    WHERE (p_cap_type = 'all_cap' OR calc_cap = p_cap_type)
	),
	
	-- 3. Get the latest date
	symbol_latest_date AS (
	    SELECT 
	        be.symbol, 
	        MAX(be.value_date) as max_date
	    FROM public.best_idea be
	    JOIN filtered_universe fu ON be.symbol = fu.symbol,
	    constants
	    WHERE be.value_date <= p_as_of_date
	      AND be.value_date >= p_as_of_date - lookback
	      AND (cardinality(p_provider_etf_ids) = 0 OR be.provider_etf_id = ANY(p_provider_etf_ids))
	    GROUP BY be.symbol
	),

	-- 4. Get the best rank the max date
	symbol_targets AS (
	    SELECT DISTINCT ON (be.symbol)
	        be.symbol,
	        be.value_date AS max_date,
	        be.ranking AS best_ranking
	    FROM public.best_idea be
	    JOIN symbol_latest_date sld ON be.symbol = sld.symbol 
	                               AND be.value_date = sld.max_date
	    WHERE be.ranking <= p_ranking_level
	      AND (cardinality(p_provider_etf_ids) = 0 OR be.provider_etf_id = ANY(p_provider_etf_ids))
	    ORDER BY be.symbol, be.ranking ASC -- Takes the best rank available on the latest date
	)
	
	-- 5. Final aggregation using the pre-filtered targets
	SELECT
	    be.symbol::TEXT,
	    st.best_ranking AS ranking,
	    COUNT(DISTINCT be.provider_etf_id) AS appearances,
	    MAX(be.delta) AS max_delta,
	    (array_agg(be.provider_etf_id ORDER BY be.delta DESC))[1] AS source_etf_id,
	    array_agg(DISTINCT be.provider_etf_id) AS all_provider_ids
	
	FROM public.best_idea be
	JOIN symbol_targets st ON be.symbol = st.symbol 
	                       AND be.value_date = st.max_date 
	                       AND be.ranking = st.best_ranking
	GROUP BY be.symbol, st.best_ranking
	ORDER BY st.best_ranking, appearances DESC, max_delta DESC
	;

$$;


ALTER FUNCTION public.get_best_ideas_by_ranking(p_ranking_level integer, p_style_type text, p_cap_type text, p_as_of_date date, p_provider_etf_ids integer[]) OWNER TO admin;

--
-- TOC entry 272 (class 1255 OID 250807)
-- Name: perf_get_alpah_compounded(); Type: FUNCTION; Schema: public; Owner: admin
--

CREATE FUNCTION public.perf_get_alpah_compounded() RETURNS TABLE(account_id integer, benchmark_symbol text, total_strategy_return numeric, total_benchmark_return numeric, compounded_alpha numeric)
    LANGUAGE sql
    AS $$
    SELECT 
        account_id,
        benchmark_symbol,
        -- Total Strategy Return: (Last Value / First Value) - 1
        (MAX(strategy_indexed_value) FILTER (WHERE performance_date = global_max_date) / 
         NULLIF(MIN(strategy_indexed_value) FILTER (WHERE performance_date = global_min_date), 0)) - 1 AS total_strategy_return,
        
        -- Total Benchmark Return: (Last Value / First Value) - 1
        (MAX(benchmark_indexed_value) FILTER (WHERE performance_date = global_max_date) / 
         NULLIF(MIN(benchmark_indexed_value) FILTER (WHERE performance_date = global_min_date), 0)) - 1 AS total_benchmark_return,
        
        -- Compounded Alpha: Total Strategy - Total Benchmark
        ((MAX(strategy_indexed_value) FILTER (WHERE performance_date = global_max_date) / 
          NULLIF(MIN(strategy_indexed_value) FILTER (WHERE performance_date = global_min_date), 0)) - 1) -
        ((MAX(benchmark_indexed_value) FILTER (WHERE performance_date = global_max_date) / 
          NULLIF(MIN(benchmark_indexed_value) FILTER (WHERE performance_date = global_min_date), 0)) - 1) AS compounded_alpha
    FROM (
        SELECT *,
            MAX(performance_date) OVER(PARTITION BY account_id, benchmark_symbol) as global_max_date,
            MIN(performance_date) OVER(PARTITION BY account_id, benchmark_symbol) as global_min_date
        FROM public.account_benchmark_comparison
    ) sub
    GROUP BY account_id, benchmark_symbol;
$$;


ALTER FUNCTION public.perf_get_alpah_compounded() OWNER TO admin;

--
-- TOC entry 268 (class 1255 OID 250803)
-- Name: perf_get_alpha_annual(); Type: FUNCTION; Schema: public; Owner: admin
--

CREATE FUNCTION public.perf_get_alpha_annual() RETURNS TABLE(account_id integer, benchmark_symbol text, performance_year double precision, annual_strategy_return numeric, annual_benchmark_return numeric, annual_alpha numeric)
    LANGUAGE sql
    AS $$
    SELECT 
        account_id,
        benchmark_symbol,
        EXTRACT(YEAR FROM performance_date) AS year,
        -- (End Value / Start Value) - 1
        (MAX(strategy_indexed_value) FILTER (WHERE performance_date = max_date) / 
         NULLIF(MIN(strategy_indexed_value) FILTER (WHERE performance_date = min_date), 0)) - 1 AS annual_strategy_return,
        
        (MAX(benchmark_indexed_value) FILTER (WHERE performance_date = max_date) / 
         NULLIF(MIN(benchmark_indexed_value) FILTER (WHERE performance_date = min_date), 0)) - 1 AS annual_benchmark_return,
        
        -- Annual Alpha
        ((MAX(strategy_indexed_value) FILTER (WHERE performance_date = max_date) / 
          NULLIF(MIN(strategy_indexed_value) FILTER (WHERE performance_date = min_date), 0)) - 1) -
        ((MAX(benchmark_indexed_value) FILTER (WHERE performance_date = max_date) / 
          NULLIF(MIN(benchmark_indexed_value) FILTER (WHERE performance_date = min_date), 0)) - 1) AS annual_alpha
    FROM (
        SELECT *,
            MAX(performance_date) OVER(PARTITION BY account_id, benchmark_symbol, EXTRACT(YEAR FROM performance_date)) as max_date,
            MIN(performance_date) OVER(PARTITION BY account_id, benchmark_symbol, EXTRACT(YEAR FROM performance_date)) as min_date
        FROM public.account_benchmark_comparison
    ) sub
    GROUP BY account_id, benchmark_symbol, year
    ORDER BY year;
$$;


ALTER FUNCTION public.perf_get_alpha_annual() OWNER TO admin;

--
-- TOC entry 271 (class 1255 OID 250893)
-- Name: perf_get_alpha_monthly(); Type: FUNCTION; Schema: public; Owner: admin
--

CREATE FUNCTION public.perf_get_alpha_monthly() RETURNS TABLE(account_id integer, benchmark_symbol text, performance_year double precision, performance_month double precision, monthly_strategy_return numeric, monthly_benchmark_return numeric, monthly_alpha numeric)
    LANGUAGE sql
    AS $$
    SELECT 
        account_id,
        benchmark_symbol,
        EXTRACT(YEAR FROM performance_date) AS year,
        EXTRACT(MONTH FROM performance_date) AS month,
        -- (End Value / Start Value) - 1
        (MAX(strategy_indexed_value) FILTER (WHERE performance_date = max_date) / 
         NULLIF(MIN(strategy_indexed_value) FILTER (WHERE performance_date = min_date), 0)) - 1 AS monthly_strategy_return,
        
        (MAX(benchmark_indexed_value) FILTER (WHERE performance_date = max_date) / 
         NULLIF(MIN(benchmark_indexed_value) FILTER (WHERE performance_date = min_date), 0)) - 1 AS monthly_benchmark_return,
        
        -- Monthly Alpha
        ((MAX(strategy_indexed_value) FILTER (WHERE performance_date = max_date) / 
          NULLIF(MIN(strategy_indexed_value) FILTER (WHERE performance_date = min_date), 0)) - 1) -
        ((MAX(benchmark_indexed_value) FILTER (WHERE performance_date = max_date) / 
          NULLIF(MIN(benchmark_indexed_value) FILTER (WHERE performance_date = min_date), 0)) - 1) AS monthly_alpha
    FROM (
        SELECT *,
            MAX(performance_date) OVER(PARTITION BY account_id, benchmark_symbol, DATE_TRUNC('month', performance_date)) as max_date,
            MIN(performance_date) OVER(PARTITION BY account_id, benchmark_symbol, DATE_TRUNC('month', performance_date)) as min_date
        FROM public.account_benchmark_comparison
    ) sub
    GROUP BY account_id, benchmark_symbol, year, month
    ORDER BY year, month;
$$;


ALTER FUNCTION public.perf_get_alpha_monthly() OWNER TO admin;

--
-- TOC entry 270 (class 1255 OID 250896)
-- Name: pref_get_turnover(numeric); Type: FUNCTION; Schema: public; Owner: admin
--

CREATE FUNCTION public.pref_get_turnover(initial_deposit numeric) RETURNS TABLE(account_id integer, net_strategy_return numeric, total_buy_volume numeric, total_sell_volume numeric, buy_pct_of_return numeric, sell_pct_of_return numeric)
    LANGUAGE sql
    AS $$
WITH account_bounds AS (
    -- Get the starting and ending total_value for each account
    SELECT 
        p.account_id,
        (SELECT total_value FROM public.account_performance_daily 
         WHERE account_id = p.account_id ORDER BY performance_date ASC LIMIT 1) as first_value,
        (SELECT total_value FROM public.account_performance_daily 
         WHERE account_id = p.account_id ORDER BY performance_date DESC LIMIT 1) as last_value
    FROM public.account_performance_daily p
    GROUP BY p.account_id
),
trade_splits AS (
    -- Aggregate Buys and Sells separately, excluding the initial deposit constant from Buys
    SELECT 
        t.account_id,
        SUM(CASE WHEN t.side = 'BUY' THEN t.total_amount ELSE 0 END) - initial_deposit as total_buy_volume,
        SUM(CASE WHEN t.side = 'SELL' THEN t.total_amount ELSE 0 END) as total_sell_volume
    FROM public.account_trade t
    GROUP BY t.account_id
)
SELECT 
    b.account_id,
    (b.last_value - b.first_value) AS net_strategy_return,
    t.total_buy_volume,
    t.total_sell_volume,
    -- Buy volume as % of net return
    CASE 
        WHEN (b.last_value - b.first_value) = 0 THEN 0
        ELSE (t.total_buy_volume / NULLIF((b.last_value + b.first_value)/2, 0)) * 100 
    END,
    -- Sell volume as % of net return
    CASE 
        WHEN (b.last_value - b.first_value) = 0 THEN 0
        ELSE (t.total_sell_volume / NULLIF((b.last_value + b.first_value)/2, 0)) * 100 
    END
FROM account_bounds b
JOIN trade_splits t ON b.account_id = t.account_id;
$$;


ALTER FUNCTION public.pref_get_turnover(initial_deposit numeric) OWNER TO admin;

--
-- TOC entry 269 (class 1255 OID 251136)
-- Name: sync_categorize_ticker(); Type: FUNCTION; Schema: public; Owner: admin
--

CREATE FUNCTION public.sync_categorize_ticker() RETURNS integer
    LANGUAGE plpgsql
    AS $$
DECLARE
    affected_count integer;
BEGIN

    WITH classifications AS (
        SELECT
            h.ticker,
            e.style_type,
            e.cap_type,
            COUNT(*) AS freq
        FROM categorize_etf_holding h
        JOIN categorize_etf e
            ON e.id = h.categorize_etf_id
        WHERE h.ticker IS NOT NULL
          AND e.style_type IS NOT NULL
          AND e.cap_type IS NOT NULL
        GROUP BY h.ticker, e.style_type, e.cap_type
    ),
    ranked AS (
        SELECT
            ticker,
            style_type,
            cap_type,
            freq,
            ROW_NUMBER() OVER (
                PARTITION BY ticker
                ORDER BY freq DESC
            ) AS rn
        FROM classifications
    ),
    best_match AS (
        SELECT
            ticker,
            style_type,
            cap_type
        FROM ranked
        WHERE rn = 1
    )

    INSERT INTO categorize_ticker (
        symbol,
        style_type,
        cap_type,
        last_update
    )
    SELECT
        ticker,
        style_type,
        cap_type,
        now()
    FROM best_match

    ON CONFLICT (symbol)
    DO UPDATE
    SET
        style_type = EXCLUDED.style_type,
        cap_type   = EXCLUDED.cap_type,
        last_update = now();

    GET DIAGNOSTICS affected_count = ROW_COUNT;

    RETURN affected_count;

END;
$$;


ALTER FUNCTION public.sync_categorize_ticker() OWNER TO admin;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 239 (class 1259 OID 250014)
-- Name: account; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.account (
    id integer NOT NULL,
    created_at timestamp without time zone DEFAULT (now() AT TIME ZONE 'utc'::text),
    name text NOT NULL,
    base_currency character varying(3) DEFAULT 'USD'::character varying,
    strategy_fund_id integer
);


ALTER TABLE public.account OWNER TO admin;

--
-- TOC entry 240 (class 1259 OID 250028)
-- Name: account_benchmark_comparison; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.account_benchmark_comparison (
    account_id integer NOT NULL,
    benchmark_symbol text NOT NULL,
    performance_date date NOT NULL,
    strategy_indexed_value numeric(20,6) DEFAULT 1.0,
    benchmark_indexed_value numeric(20,6) DEFAULT 1.0,
    daily_alpha numeric(20,6)
);


ALTER TABLE public.account_benchmark_comparison OWNER TO admin;

--
-- TOC entry 242 (class 1259 OID 250043)
-- Name: account_cash_ledger; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.account_cash_ledger (
    id integer NOT NULL,
    account_id integer NOT NULL,
    transaction_date date NOT NULL,
    amount numeric(18,4) NOT NULL,
    entry_type text NOT NULL,
    description text
);


ALTER TABLE public.account_cash_ledger OWNER TO admin;

--
-- TOC entry 241 (class 1259 OID 250042)
-- Name: account_cash_ledger_id_seq; Type: SEQUENCE; Schema: public; Owner: admin
--

ALTER TABLE public.account_cash_ledger ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.account_cash_ledger_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 243 (class 1259 OID 250060)
-- Name: account_holding_daily; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.account_holding_daily (
    account_id integer NOT NULL,
    holding_date date NOT NULL,
    symbol text NOT NULL,
    quantity numeric(18,8) NOT NULL,
    cost_basis numeric(18,4) NOT NULL,
    market_value numeric(18,4) NOT NULL,
    weight_percentage numeric(5,4) NOT NULL
);


ALTER TABLE public.account_holding_daily OWNER TO admin;

--
-- TOC entry 238 (class 1259 OID 250013)
-- Name: account_id_seq; Type: SEQUENCE; Schema: public; Owner: admin
--

ALTER TABLE public.account ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.account_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 244 (class 1259 OID 250077)
-- Name: account_performance_daily; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.account_performance_daily (
    account_id integer NOT NULL,
    performance_date date NOT NULL,
    total_value numeric(20,6) NOT NULL,
    cash_balance numeric(20,6) NOT NULL,
    stock_value numeric(20,6) NOT NULL,
    daily_return numeric(20,6) DEFAULT 0,
    cumulative_return numeric(10,6) DEFAULT 0,
    max_drawdown numeric(10,6) DEFAULT 0
);


ALTER TABLE public.account_performance_daily OWNER TO admin;

--
-- TOC entry 246 (class 1259 OID 250091)
-- Name: account_trade; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.account_trade (
    id integer NOT NULL,
    account_id integer NOT NULL,
    symbol text NOT NULL,
    trade_date date NOT NULL,
    side character varying(4) NOT NULL,
    quantity numeric(18,8) NOT NULL,
    price numeric(18,4) NOT NULL,
    commission numeric(18,4) DEFAULT 0,
    total_amount numeric(18,4) NOT NULL
);


ALTER TABLE public.account_trade OWNER TO admin;

--
-- TOC entry 245 (class 1259 OID 250090)
-- Name: account_trade_id_seq; Type: SEQUENCE; Schema: public; Owner: admin
--

ALTER TABLE public.account_trade ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.account_trade_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 237 (class 1259 OID 250005)
-- Name: benchmark_value; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.benchmark_value (
    symbol text NOT NULL,
    value_date date NOT NULL,
    price numeric(18,4) NOT NULL
);


ALTER TABLE public.benchmark_value OWNER TO admin;

--
-- TOC entry 229 (class 1259 OID 249775)
-- Name: best_idea; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.best_idea (
    provider_etf_id integer NOT NULL,
    symbol character varying(32) NOT NULL,
    value_date date NOT NULL,
    etf_weight double precision,
    benchmark_weight double precision,
    delta double precision,
    ranking integer
);


ALTER TABLE public.best_idea OWNER TO admin;

--
-- TOC entry 233 (class 1259 OID 249804)
-- Name: categorize_etf; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.categorize_etf (
    id integer NOT NULL,
    created_at timestamp without time zone DEFAULT (now() AT TIME ZONE 'utc'::text) NOT NULL,
    name text,
    cap_type text,
    style_type text,
    url text NOT NULL,
    wait_pre_events text,
    wait_post_events text,
    events jsonb,
    trigger_download jsonb,
    mapping jsonb,
    file_format character varying(10),
    last_downloaded timestamp without time zone
);


ALTER TABLE public.categorize_etf OWNER TO admin;

--
-- TOC entry 248 (class 1259 OID 250185)
-- Name: categorize_etf_holding; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.categorize_etf_holding (
    id integer NOT NULL,
    created_at timestamp without time zone DEFAULT (now() AT TIME ZONE 'utc'::text) NOT NULL,
    categorize_etf_id integer NOT NULL,
    holding_date timestamp without time zone NOT NULL,
    ticker text
);


ALTER TABLE public.categorize_etf_holding OWNER TO admin;

--
-- TOC entry 247 (class 1259 OID 250184)
-- Name: categorize_etf_holding_id_seq; Type: SEQUENCE; Schema: public; Owner: admin
--

ALTER TABLE public.categorize_etf_holding ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.categorize_etf_holding_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 232 (class 1259 OID 249803)
-- Name: categorize_etf_id_seq; Type: SEQUENCE; Schema: public; Owner: admin
--

ALTER TABLE public.categorize_etf ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.categorize_etf_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 250 (class 1259 OID 250351)
-- Name: categorize_ticker; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.categorize_ticker (
    symbol character varying(32) NOT NULL,
    style_type character varying(16),
    cap_type character varying(16),
    sector text,
    market_cap bigint,
    factors jsonb,
    last_update timestamp without time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.categorize_ticker OWNER TO admin;

--
-- TOC entry 226 (class 1259 OID 249733)
-- Name: fund; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.fund (
    id integer NOT NULL,
    created_at timestamp without time zone DEFAULT (now() AT TIME ZONE 'utc'::text) NOT NULL,
    name text,
    strategy jsonb,
    active boolean DEFAULT true
);


ALTER TABLE public.fund OWNER TO admin;

--
-- TOC entry 227 (class 1259 OID 249741)
-- Name: fund_holding; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.fund_holding (
    fund_id integer NOT NULL,
    symbol text NOT NULL,
    holding_date date NOT NULL,
    ranking integer NOT NULL,
    source_etf_id integer,
    max_delta double precision
);


ALTER TABLE public.fund_holding OWNER TO admin;

--
-- TOC entry 228 (class 1259 OID 249758)
-- Name: fund_holding_change; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.fund_holding_change (
    fund_id integer NOT NULL,
    symbol text NOT NULL,
    change_date date NOT NULL,
    direction character varying(5) NOT NULL,
    ranking integer,
    appearances integer,
    max_delta double precision,
    top_delta_provider_etf_id integer,
    all_provider_etf_ids integer[],
    reason text
);


ALTER TABLE public.fund_holding_change OWNER TO admin;

--
-- TOC entry 225 (class 1259 OID 249732)
-- Name: fund_id_seq; Type: SEQUENCE; Schema: public; Owner: admin
--

ALTER TABLE public.fund ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.fund_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 235 (class 1259 OID 249918)
-- Name: interest_rate_config; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.interest_rate_config (
    id integer NOT NULL,
    effective_date date NOT NULL,
    annual_rate numeric(5,4) NOT NULL
);


ALTER TABLE public.interest_rate_config OWNER TO admin;

--
-- TOC entry 234 (class 1259 OID 249917)
-- Name: interest_rate_config_id_seq; Type: SEQUENCE; Schema: public; Owner: admin
--

ALTER TABLE public.interest_rate_config ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.interest_rate_config_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 231 (class 1259 OID 249795)
-- Name: log; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.log (
    id integer NOT NULL,
    created_at timestamp without time zone DEFAULT (now() AT TIME ZONE 'utc'::text) NOT NULL,
    process character varying(20),
    log_type character varying(20),
    code character varying(20),
    msg text
);


ALTER TABLE public.log OWNER TO admin;

--
-- TOC entry 230 (class 1259 OID 249794)
-- Name: log_id_seq; Type: SEQUENCE; Schema: public; Owner: admin
--

ALTER TABLE public.log ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.log_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 218 (class 1259 OID 249673)
-- Name: provider; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.provider (
    id integer NOT NULL,
    created_at timestamp without time zone DEFAULT (now() AT TIME ZONE 'utc'::text) NOT NULL,
    disabled boolean DEFAULT false NOT NULL,
    disabled_reason text,
    name text,
    domain text,
    file_format character varying(10)
);


ALTER TABLE public.provider OWNER TO admin;

--
-- TOC entry 220 (class 1259 OID 249683)
-- Name: provider_etf; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.provider_etf (
    id integer NOT NULL,
    created_at timestamp without time zone DEFAULT (now() AT TIME ZONE 'utc'::text) NOT NULL,
    provider_id integer NOT NULL,
    disabled boolean DEFAULT false NOT NULL,
    disabled_reason text,
    name text,
    isin text,
    ticker text,
    cap_type text,
    style_type text,
    benchmark text,
    trading_since timestamp without time zone,
    number_of_managers integer,
    url text NOT NULL,
    file_format character varying(10)
);


ALTER TABLE public.provider_etf OWNER TO admin;

--
-- TOC entry 222 (class 1259 OID 249699)
-- Name: provider_etf_holding; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.provider_etf_holding (
    id integer NOT NULL,
    created_at timestamp without time zone DEFAULT (now() AT TIME ZONE 'utc'::text) NOT NULL,
    provider_etf_id integer NOT NULL,
    holding_date timestamp without time zone NOT NULL,
    ticker text,
    shares double precision,
    market_value double precision,
    weight double precision
);


ALTER TABLE public.provider_etf_holding OWNER TO admin;

--
-- TOC entry 252 (class 1259 OID 250501)
-- Name: provider_etf_holding_factset; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.provider_etf_holding_factset (
    id integer NOT NULL,
    created_at timestamp without time zone NOT NULL,
    provider_etf_id integer NOT NULL,
    holding_date timestamp without time zone NOT NULL,
    ticker text,
    shares double precision,
    market_value double precision,
    weight double precision
);


ALTER TABLE public.provider_etf_holding_factset OWNER TO admin;

--
-- TOC entry 251 (class 1259 OID 250500)
-- Name: provider_etf_holding_factset_id_seq; Type: SEQUENCE; Schema: public; Owner: admin
--

ALTER TABLE public.provider_etf_holding_factset ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.provider_etf_holding_factset_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 221 (class 1259 OID 249698)
-- Name: provider_etf_holding_id_seq; Type: SEQUENCE; Schema: public; Owner: admin
--

ALTER TABLE public.provider_etf_holding ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.provider_etf_holding_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 254 (class 1259 OID 250516)
-- Name: provider_etf_holding_morningstar; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.provider_etf_holding_morningstar (
    id integer NOT NULL,
    created_at timestamp without time zone DEFAULT (now() AT TIME ZONE 'utc'::text) NOT NULL,
    provider_etf_id integer NOT NULL,
    holding_date timestamp without time zone NOT NULL,
    ticker text,
    shares double precision,
    market_value double precision,
    weight double precision
);


ALTER TABLE public.provider_etf_holding_morningstar OWNER TO admin;

--
-- TOC entry 253 (class 1259 OID 250515)
-- Name: provider_etf_holding_morningstar_id_seq; Type: SEQUENCE; Schema: public; Owner: admin
--

ALTER TABLE public.provider_etf_holding_morningstar ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.provider_etf_holding_morningstar_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 256 (class 1259 OID 251016)
-- Name: provider_etf_holding_our_data; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.provider_etf_holding_our_data (
    id integer NOT NULL,
    created_at timestamp without time zone DEFAULT (now() AT TIME ZONE 'utc'::text) NOT NULL,
    provider_etf_id integer NOT NULL,
    holding_date timestamp without time zone NOT NULL,
    ticker text,
    shares double precision,
    market_value double precision,
    weight double precision
);


ALTER TABLE public.provider_etf_holding_our_data OWNER TO admin;

--
-- TOC entry 255 (class 1259 OID 251015)
-- Name: provider_etf_holding_our_data_id_seq; Type: SEQUENCE; Schema: public; Owner: admin
--

ALTER TABLE public.provider_etf_holding_our_data ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.provider_etf_holding_our_data_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 219 (class 1259 OID 249682)
-- Name: provider_etf_id_seq; Type: SEQUENCE; Schema: public; Owner: admin
--

ALTER TABLE public.provider_etf ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.provider_etf_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 217 (class 1259 OID 249672)
-- Name: provider_id_seq; Type: SEQUENCE; Schema: public; Owner: admin
--

ALTER TABLE public.provider ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.provider_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 223 (class 1259 OID 249714)
-- Name: ticker; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.ticker (
    symbol character varying(32) NOT NULL,
    created_at timestamp without time zone DEFAULT (now() AT TIME ZONE 'utc'::text) NOT NULL,
    source character varying(16),
    style_type character varying(16),
    cap_type character varying(16),
    type_from character varying(16),
    isin character varying(16),
    cik character varying(16),
    exchange character varying(32),
    name text,
    industry text,
    sector text,
    invalid text
);


ALTER TABLE public.ticker OWNER TO admin;

--
-- TOC entry 236 (class 1259 OID 249947)
-- Name: ticker_dividend_history; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.ticker_dividend_history (
    symbol text NOT NULL,
    ex_date date NOT NULL,
    amount_per_share numeric(18,4) NOT NULL
);


ALTER TABLE public.ticker_dividend_history OWNER TO admin;

--
-- TOC entry 249 (class 1259 OID 250232)
-- Name: ticker_split_history; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.ticker_split_history (
    symbol text NOT NULL,
    date date NOT NULL,
    numerator numeric(18,4) NOT NULL,
    denominator numeric(18,4) NOT NULL
);


ALTER TABLE public.ticker_split_history OWNER TO admin;

--
-- TOC entry 224 (class 1259 OID 249722)
-- Name: ticker_value; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.ticker_value (
    symbol character varying(32) NOT NULL,
    value_date date NOT NULL,
    stock_price double precision,
    market_cap double precision
);


ALTER TABLE public.ticker_value OWNER TO admin;

--
-- TOC entry 4921 (class 2606 OID 250036)
-- Name: account_benchmark_comparison account_benchmark_comparison_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.account_benchmark_comparison
    ADD CONSTRAINT account_benchmark_comparison_pkey PRIMARY KEY (account_id, benchmark_symbol, performance_date);


--
-- TOC entry 4923 (class 2606 OID 250049)
-- Name: account_cash_ledger account_cash_ledger_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.account_cash_ledger
    ADD CONSTRAINT account_cash_ledger_pkey PRIMARY KEY (id);


--
-- TOC entry 4925 (class 2606 OID 250066)
-- Name: account_holding_daily account_holding_daily_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.account_holding_daily
    ADD CONSTRAINT account_holding_daily_pkey PRIMARY KEY (account_id, holding_date, symbol);


--
-- TOC entry 4927 (class 2606 OID 250084)
-- Name: account_performance_daily account_performance_daily_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.account_performance_daily
    ADD CONSTRAINT account_performance_daily_pkey PRIMARY KEY (account_id, performance_date);


--
-- TOC entry 4919 (class 2606 OID 250022)
-- Name: account account_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.account
    ADD CONSTRAINT account_pkey PRIMARY KEY (id);


--
-- TOC entry 4929 (class 2606 OID 250098)
-- Name: account_trade account_trade_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.account_trade
    ADD CONSTRAINT account_trade_pkey PRIMARY KEY (id);


--
-- TOC entry 4916 (class 2606 OID 250011)
-- Name: benchmark_value benchmark_value_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.benchmark_value
    ADD CONSTRAINT benchmark_value_pkey PRIMARY KEY (symbol, value_date);


--
-- TOC entry 4903 (class 2606 OID 249779)
-- Name: best_idea best_idea_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.best_idea
    ADD CONSTRAINT best_idea_pkey PRIMARY KEY (provider_etf_id, symbol, value_date);


--
-- TOC entry 4931 (class 2606 OID 250192)
-- Name: categorize_etf_holding categorize_etf_holding_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.categorize_etf_holding
    ADD CONSTRAINT categorize_etf_holding_pkey PRIMARY KEY (id);


--
-- TOC entry 4910 (class 2606 OID 249811)
-- Name: categorize_etf categorize_etf_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.categorize_etf
    ADD CONSTRAINT categorize_etf_pkey PRIMARY KEY (id);


--
-- TOC entry 4901 (class 2606 OID 249764)
-- Name: fund_holding_change fund_holding_change_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.fund_holding_change
    ADD CONSTRAINT fund_holding_change_pkey PRIMARY KEY (fund_id, change_date, symbol, direction);


--
-- TOC entry 4899 (class 2606 OID 249747)
-- Name: fund_holding fund_holding_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.fund_holding
    ADD CONSTRAINT fund_holding_pkey PRIMARY KEY (fund_id, holding_date, symbol);


--
-- TOC entry 4897 (class 2606 OID 249740)
-- Name: fund fund_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.fund
    ADD CONSTRAINT fund_pkey PRIMARY KEY (id);


--
-- TOC entry 4912 (class 2606 OID 249922)
-- Name: interest_rate_config interest_rate_config_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.interest_rate_config
    ADD CONSTRAINT interest_rate_config_pkey PRIMARY KEY (id);


--
-- TOC entry 4908 (class 2606 OID 249802)
-- Name: log log_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.log
    ADD CONSTRAINT log_pkey PRIMARY KEY (id);


--
-- TOC entry 4937 (class 2606 OID 250358)
-- Name: categorize_ticker pk_categorize_ticker; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.categorize_ticker
    ADD CONSTRAINT pk_categorize_ticker PRIMARY KEY (symbol);


--
-- TOC entry 4893 (class 2606 OID 249721)
-- Name: ticker pk_ticker; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.ticker
    ADD CONSTRAINT pk_ticker PRIMARY KEY (symbol);


--
-- TOC entry 4895 (class 2606 OID 249726)
-- Name: ticker_value pk_ticker_value; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.ticker_value
    ADD CONSTRAINT pk_ticker_value PRIMARY KEY (symbol, value_date);


--
-- TOC entry 4941 (class 2606 OID 250507)
-- Name: provider_etf_holding_factset provider_etf_holding_factset_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.provider_etf_holding_factset
    ADD CONSTRAINT provider_etf_holding_factset_pkey PRIMARY KEY (id);


--
-- TOC entry 4945 (class 2606 OID 250523)
-- Name: provider_etf_holding_morningstar provider_etf_holding_morningstar_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.provider_etf_holding_morningstar
    ADD CONSTRAINT provider_etf_holding_morningstar_pkey PRIMARY KEY (id);


--
-- TOC entry 4949 (class 2606 OID 251023)
-- Name: provider_etf_holding_our_data provider_etf_holding_our_data_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.provider_etf_holding_our_data
    ADD CONSTRAINT provider_etf_holding_our_data_pkey PRIMARY KEY (id);


--
-- TOC entry 4891 (class 2606 OID 249706)
-- Name: provider_etf_holding provider_etf_holding_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.provider_etf_holding
    ADD CONSTRAINT provider_etf_holding_pkey PRIMARY KEY (id);


--
-- TOC entry 4887 (class 2606 OID 249691)
-- Name: provider_etf provider_etf_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.provider_etf
    ADD CONSTRAINT provider_etf_pkey PRIMARY KEY (id);


--
-- TOC entry 4884 (class 2606 OID 249681)
-- Name: provider provider_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.provider
    ADD CONSTRAINT provider_pkey PRIMARY KEY (id);


--
-- TOC entry 4914 (class 2606 OID 249953)
-- Name: ticker_dividend_history ticker_dividend_history_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.ticker_dividend_history
    ADD CONSTRAINT ticker_dividend_history_pkey PRIMARY KEY (symbol, ex_date);


--
-- TOC entry 4935 (class 2606 OID 250238)
-- Name: ticker_split_history ticker_split_history_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.ticker_split_history
    ADD CONSTRAINT ticker_split_history_pkey PRIMARY KEY (symbol, date);


--
-- TOC entry 4904 (class 1259 OID 249790)
-- Name: fki_fk_best_idea_provider_etf_id; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX fki_fk_best_idea_provider_etf_id ON public.best_idea USING btree (provider_etf_id) WITH (fillfactor='100', deduplicate_items='true');


--
-- TOC entry 4905 (class 1259 OID 249791)
-- Name: fki_fk_best_idea_symbol; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX fki_fk_best_idea_symbol ON public.best_idea USING btree (symbol) WITH (fillfactor='100', deduplicate_items='true');


--
-- TOC entry 4906 (class 1259 OID 249792)
-- Name: fki_fk_best_idea_value_date; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX fki_fk_best_idea_value_date ON public.best_idea USING btree (value_date) WITH (fillfactor='100', deduplicate_items='true');


--
-- TOC entry 4932 (class 1259 OID 250198)
-- Name: fki_fk_categorize_etf_holding_categorize_etf_id; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX fki_fk_categorize_etf_holding_categorize_etf_id ON public.categorize_etf_holding USING btree (categorize_etf_id) WITH (fillfactor='100', deduplicate_items='true');


--
-- TOC entry 4938 (class 1259 OID 250513)
-- Name: fki_fk_provider_etf_holding_factset_provider_etf_id; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX fki_fk_provider_etf_holding_factset_provider_etf_id ON public.provider_etf_holding_factset USING btree (provider_etf_id) WITH (fillfactor='100', deduplicate_items='true');


--
-- TOC entry 4942 (class 1259 OID 250529)
-- Name: fki_fk_provider_etf_holding_morningstar_provider_etf_id; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX fki_fk_provider_etf_holding_morningstar_provider_etf_id ON public.provider_etf_holding_morningstar USING btree (provider_etf_id) WITH (fillfactor='100', deduplicate_items='true');


--
-- TOC entry 4946 (class 1259 OID 251029)
-- Name: fki_fk_provider_etf_holding_our_data_provider_etf_id; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX fki_fk_provider_etf_holding_our_data_provider_etf_id ON public.provider_etf_holding_our_data USING btree (provider_etf_id) WITH (fillfactor='100', deduplicate_items='true');


--
-- TOC entry 4888 (class 1259 OID 249712)
-- Name: fki_fk_provider_etf_holding_provider_etf_id; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX fki_fk_provider_etf_holding_provider_etf_id ON public.provider_etf_holding USING btree (provider_etf_id) WITH (fillfactor='100', deduplicate_items='true');


--
-- TOC entry 4885 (class 1259 OID 249697)
-- Name: fki_fk_provider_etf_provider_id; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX fki_fk_provider_etf_provider_id ON public.provider_etf USING btree (provider_id) WITH (fillfactor='100', deduplicate_items='true');


--
-- TOC entry 4917 (class 1259 OID 250012)
-- Name: idx_benchmark_price_lookup; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX idx_benchmark_price_lookup ON public.benchmark_value USING btree (symbol, value_date);


--
-- TOC entry 4933 (class 1259 OID 250199)
-- Name: idx_categorize_etf_holding_trade_date; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX idx_categorize_etf_holding_trade_date ON public.categorize_etf_holding USING btree (holding_date) WITH (fillfactor='100', deduplicate_items='true');


--
-- TOC entry 4939 (class 1259 OID 250514)
-- Name: idx_provider_etf_holding_factset_holding_date; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX idx_provider_etf_holding_factset_holding_date ON public.provider_etf_holding_factset USING btree (holding_date) WITH (fillfactor='100', deduplicate_items='true');


--
-- TOC entry 4889 (class 1259 OID 249713)
-- Name: idx_provider_etf_holding_holding_date; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX idx_provider_etf_holding_holding_date ON public.provider_etf_holding USING btree (holding_date) WITH (fillfactor='100', deduplicate_items='true');


--
-- TOC entry 4943 (class 1259 OID 250530)
-- Name: idx_provider_etf_holding_morningstar_holding_date; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX idx_provider_etf_holding_morningstar_holding_date ON public.provider_etf_holding_morningstar USING btree (holding_date) WITH (fillfactor='100', deduplicate_items='true');


--
-- TOC entry 4947 (class 1259 OID 251030)
-- Name: idx_provider_etf_holding_our_data_holding_date; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX idx_provider_etf_holding_our_data_holding_date ON public.provider_etf_holding_our_data USING btree (holding_date) WITH (fillfactor='100', deduplicate_items='true');


--
-- TOC entry 4962 (class 2606 OID 250050)
-- Name: account_cash_ledger account_cash_ledger_account_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.account_cash_ledger
    ADD CONSTRAINT account_cash_ledger_account_id_fkey FOREIGN KEY (account_id) REFERENCES public.account(id);


--
-- TOC entry 4967 (class 2606 OID 250099)
-- Name: account_trade account_trade_account_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.account_trade
    ADD CONSTRAINT account_trade_account_id_fkey FOREIGN KEY (account_id) REFERENCES public.account(id);


--
-- TOC entry 4968 (class 2606 OID 250104)
-- Name: account_trade account_trade_symbol_fkey; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.account_trade
    ADD CONSTRAINT account_trade_symbol_fkey FOREIGN KEY (symbol) REFERENCES public.ticker(symbol);


--
-- TOC entry 4963 (class 2606 OID 250055)
-- Name: account_cash_ledger fk_account_cash_ledger_account; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.account_cash_ledger
    ADD CONSTRAINT fk_account_cash_ledger_account FOREIGN KEY (account_id) REFERENCES public.account(id);


--
-- TOC entry 4960 (class 2606 OID 250023)
-- Name: account fk_account_fund; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.account
    ADD CONSTRAINT fk_account_fund FOREIGN KEY (strategy_fund_id) REFERENCES public.fund(id) ON DELETE CASCADE;


--
-- TOC entry 4957 (class 2606 OID 249780)
-- Name: best_idea fk_best_idea_provider_etf_id; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.best_idea
    ADD CONSTRAINT fk_best_idea_provider_etf_id FOREIGN KEY (provider_etf_id) REFERENCES public.provider_etf(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4958 (class 2606 OID 249785)
-- Name: best_idea fk_best_idea_symbol; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.best_idea
    ADD CONSTRAINT fk_best_idea_symbol FOREIGN KEY (symbol) REFERENCES public.ticker(symbol) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4969 (class 2606 OID 250193)
-- Name: categorize_etf_holding fk_categorize_etf_holding_categorize_etf_id; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.categorize_etf_holding
    ADD CONSTRAINT fk_categorize_etf_holding_categorize_etf_id FOREIGN KEY (categorize_etf_id) REFERENCES public.categorize_etf(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4961 (class 2606 OID 250037)
-- Name: account_benchmark_comparison fk_comparison_account; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.account_benchmark_comparison
    ADD CONSTRAINT fk_comparison_account FOREIGN KEY (account_id) REFERENCES public.account(id) ON DELETE CASCADE;


--
-- TOC entry 4955 (class 2606 OID 249765)
-- Name: fund_holding_change fk_fund_holding_change_fund_id; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.fund_holding_change
    ADD CONSTRAINT fk_fund_holding_change_fund_id FOREIGN KEY (fund_id) REFERENCES public.fund(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4956 (class 2606 OID 249770)
-- Name: fund_holding_change fk_fund_holding_change_symbol; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.fund_holding_change
    ADD CONSTRAINT fk_fund_holding_change_symbol FOREIGN KEY (symbol) REFERENCES public.ticker(symbol) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4953 (class 2606 OID 249748)
-- Name: fund_holding fk_fund_holding_fund_id; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.fund_holding
    ADD CONSTRAINT fk_fund_holding_fund_id FOREIGN KEY (fund_id) REFERENCES public.fund(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4954 (class 2606 OID 249753)
-- Name: fund_holding fk_fund_holding_symbol; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.fund_holding
    ADD CONSTRAINT fk_fund_holding_symbol FOREIGN KEY (symbol) REFERENCES public.ticker(symbol) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4964 (class 2606 OID 250067)
-- Name: account_holding_daily fk_holding_account; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.account_holding_daily
    ADD CONSTRAINT fk_holding_account FOREIGN KEY (account_id) REFERENCES public.account(id) ON DELETE CASCADE;


--
-- TOC entry 4965 (class 2606 OID 250072)
-- Name: account_holding_daily fk_holding_symbol; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.account_holding_daily
    ADD CONSTRAINT fk_holding_symbol FOREIGN KEY (symbol) REFERENCES public.ticker(symbol) ON DELETE CASCADE;


--
-- TOC entry 4966 (class 2606 OID 250085)
-- Name: account_performance_daily fk_performance_account; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.account_performance_daily
    ADD CONSTRAINT fk_performance_account FOREIGN KEY (account_id) REFERENCES public.account(id) ON DELETE CASCADE;


--
-- TOC entry 4971 (class 2606 OID 250508)
-- Name: provider_etf_holding_factset fk_provider_etf_holding_factset_provider_etf_id; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.provider_etf_holding_factset
    ADD CONSTRAINT fk_provider_etf_holding_factset_provider_etf_id FOREIGN KEY (provider_etf_id) REFERENCES public.provider_etf(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4972 (class 2606 OID 250524)
-- Name: provider_etf_holding_morningstar fk_provider_etf_holding_morningstar_provider_etf_id; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.provider_etf_holding_morningstar
    ADD CONSTRAINT fk_provider_etf_holding_morningstar_provider_etf_id FOREIGN KEY (provider_etf_id) REFERENCES public.provider_etf(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4973 (class 2606 OID 251024)
-- Name: provider_etf_holding_our_data fk_provider_etf_holding_our_data_provider_etf_id; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.provider_etf_holding_our_data
    ADD CONSTRAINT fk_provider_etf_holding_our_data_provider_etf_id FOREIGN KEY (provider_etf_id) REFERENCES public.provider_etf(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4951 (class 2606 OID 249707)
-- Name: provider_etf_holding fk_provider_etf_holding_provider_etf_id; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.provider_etf_holding
    ADD CONSTRAINT fk_provider_etf_holding_provider_etf_id FOREIGN KEY (provider_etf_id) REFERENCES public.provider_etf(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4950 (class 2606 OID 249692)
-- Name: provider_etf fk_provider_etf_provider_id; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.provider_etf
    ADD CONSTRAINT fk_provider_etf_provider_id FOREIGN KEY (provider_id) REFERENCES public.provider(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4952 (class 2606 OID 251174)
-- Name: ticker_value fk_ticker_symbol; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.ticker_value
    ADD CONSTRAINT fk_ticker_symbol FOREIGN KEY (symbol) REFERENCES public.ticker(symbol) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4959 (class 2606 OID 249954)
-- Name: ticker_dividend_history ticker_dividend_history_symbol_fkey; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.ticker_dividend_history
    ADD CONSTRAINT ticker_dividend_history_symbol_fkey FOREIGN KEY (symbol) REFERENCES public.ticker(symbol);


--
-- TOC entry 4970 (class 2606 OID 250239)
-- Name: ticker_split_history ticker_split_history_symbol_fkey; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.ticker_split_history
    ADD CONSTRAINT ticker_split_history_symbol_fkey FOREIGN KEY (symbol) REFERENCES public.ticker(symbol);


-- Completed on 2026-04-11 20:06:36

--
-- PostgreSQL database dump complete
--

\unrestrict 2AXA0yBiFgiN8zjZdbEel0fgHpLJFcxfwHZ5Od5G3xQoKbCqTBYiuEza9x90cxf

