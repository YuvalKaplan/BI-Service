--
-- PostgreSQL database dump
--

\restrict 9stkKcboB9lVQxEmetJv1KZXtlCFKFFbqIFZlHxWzzsvBc89LLOhv1pqkBTWEM0

-- Dumped from database version 17.6
-- Dumped by pg_dump version 18.0

-- Started on 2026-04-11 20:05:34

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
-- TOC entry 255 (class 1255 OID 251080)
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
-- TOC entry 254 (class 1255 OID 241996)
-- Name: get_latest_fund_holdings(); Type: FUNCTION; Schema: public; Owner: admin
--

CREATE FUNCTION public.get_latest_fund_holdings() RETURNS TABLE(fund_name text, holding_date date, symbol text, ticker_name text)
    LANGUAGE sql STABLE
    AS $$
    SELECT
        f.name        AS fund_name,
        fh.holding_date,
        t.symbol,
        t.name        AS ticker_name
    FROM public.fund_holding fh
    JOIN fund f   ON fh.fund_id = f.id
    JOIN ticker t ON fh.symbol = t.symbol
    WHERE fh.holding_date = (
        SELECT MAX(holding_date)
        FROM public.fund_holding
    )
    ORDER BY f.name, t.symbol;
$$;


ALTER FUNCTION public.get_latest_fund_holdings() OWNER TO admin;

--
-- TOC entry 256 (class 1255 OID 251137)
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

--
-- TOC entry 253 (class 1255 OID 241514)
-- Name: truncate_all_tables(); Type: PROCEDURE; Schema: public; Owner: admin
--

CREATE PROCEDURE public.truncate_all_tables()
    LANGUAGE plpgsql
    AS $$
DECLARE
    r RECORD;
BEGIN
    -- Loop through all user tables in the public schema
    TRUNCATE TABLE  
		public.batch_run, 
		public.batch_run_log, 
		public.best_idea, 
		public.categorize_etf,
		public.fund,
		public.fund_holding,
		public.fund_holding_change,
		public.log,
		public.provider,
		public.provider_etf,
		public.provider_etf_holding,
		public.ticker,
		public.ticker_value
		CASCADE;
		
END
$$;


ALTER PROCEDURE public.truncate_all_tables() OWNER TO admin;

--
-- TOC entry 241 (class 1255 OID 241503)
-- Name: x_d_shift_columns_template(); Type: PROCEDURE; Schema: public; Owner: admin
--

CREATE PROCEDURE public.x_d_shift_columns_template()
    LANGUAGE sql
    AS $$

-- New column:
-- ALTER TABLE public.agent_client ADD COLUMN role role_type;

-- Old Columns:
-- ALTER TABLE public.agent_client ADD COLUMN _email text COLLATE pg_catalog."default";
-- ALTER TABLE public.agent_client ADD COLUMN _firstname text COLLATE pg_catalog."default";
-- Add all the rest...

-- UPDATE public.agent_client SET 
--     _email = email,
--     _firstname = firstname,
--     -- Add all the rest...
--     _description = description;

-- ALTER TABLE public.agent_client DROP COLUMN email;
-- ALTER TABLE public.agent_client DROP COLUMN firstname;
-- Add all the rest...

-- ALTER TABLE public.agent_client RENAME COLUMN _email TO email;
-- ALTER TABLE public.agent_client RENAME COLUMN _firstname TO firstname; 
-- Add all the rest...

-- Set any columns that are not null:
-- ALTER TABLE IF EXISTS public.agent_client ALTER COLUMN email SET NOT NULL;
-- ALTER TABLE IF EXISTS public.agent_client ALTER COLUMN firstname SET NOT NULL;
-- Add all the rest...

$$;


ALTER PROCEDURE public.x_d_shift_columns_template() OWNER TO admin;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 222 (class 1259 OID 186114)
-- Name: batch_run; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.batch_run (
    id integer NOT NULL,
    created_at timestamp without time zone DEFAULT (now() AT TIME ZONE 'utc'::text) NOT NULL,
    process character varying(20),
    activation character varying(20),
    completed_at timestamp without time zone
);


ALTER TABLE public.batch_run OWNER TO admin;

--
-- TOC entry 221 (class 1259 OID 186113)
-- Name: batch_run_id_seq; Type: SEQUENCE; Schema: public; Owner: admin
--

ALTER TABLE public.batch_run ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.batch_run_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 224 (class 1259 OID 186130)
-- Name: batch_run_log; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.batch_run_log (
    id integer NOT NULL,
    created_at timestamp without time zone DEFAULT (now() AT TIME ZONE 'utc'::text) NOT NULL,
    batch_run_id integer NOT NULL,
    note text
);


ALTER TABLE public.batch_run_log OWNER TO admin;

--
-- TOC entry 223 (class 1259 OID 186129)
-- Name: batch_run_log_id_seq; Type: SEQUENCE; Schema: public; Owner: admin
--

ALTER TABLE public.batch_run_log ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.batch_run_log_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 233 (class 1259 OID 241454)
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
-- TOC entry 230 (class 1259 OID 239101)
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
-- TOC entry 237 (class 1259 OID 250201)
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
-- TOC entry 236 (class 1259 OID 250200)
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
-- TOC entry 229 (class 1259 OID 239100)
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
-- TOC entry 240 (class 1259 OID 251128)
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
-- TOC entry 235 (class 1259 OID 241479)
-- Name: fund; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.fund (
    id integer NOT NULL,
    created_at timestamp without time zone DEFAULT (now() AT TIME ZONE 'utc'::text) NOT NULL,
    name text,
    strategy jsonb,
    active boolean DEFAULT true NOT NULL
);


ALTER TABLE public.fund OWNER TO admin;

--
-- TOC entry 238 (class 1259 OID 251094)
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
-- TOC entry 239 (class 1259 OID 251111)
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
-- TOC entry 234 (class 1259 OID 241478)
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
-- TOC entry 220 (class 1259 OID 186105)
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
-- TOC entry 219 (class 1259 OID 186104)
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
-- TOC entry 218 (class 1259 OID 186062)
-- Name: provider; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.provider (
    id integer NOT NULL,
    created_at timestamp without time zone DEFAULT (now() AT TIME ZONE 'utc'::text) NOT NULL,
    disabled boolean DEFAULT false NOT NULL,
    disabled_reason text,
    name text,
    domain text,
    url_start text,
    wait_pre_events text,
    wait_post_events text,
    events jsonb,
    trigger_download jsonb,
    mapping jsonb,
    file_format character varying(10)
);


ALTER TABLE public.provider OWNER TO admin;

--
-- TOC entry 226 (class 1259 OID 192262)
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
    wait_pre_events text,
    wait_post_events text,
    events jsonb,
    trigger_download jsonb,
    mapping jsonb,
    file_format character varying(10),
    last_downloaded timestamp without time zone
);


ALTER TABLE public.provider_etf OWNER TO admin;

--
-- TOC entry 228 (class 1259 OID 192294)
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
-- TOC entry 227 (class 1259 OID 192293)
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
-- TOC entry 225 (class 1259 OID 192261)
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
-- TOC entry 217 (class 1259 OID 186061)
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
-- TOC entry 231 (class 1259 OID 239146)
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
-- TOC entry 232 (class 1259 OID 239154)
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
-- TOC entry 4832 (class 2606 OID 186137)
-- Name: batch_run_log batch_run_log_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.batch_run_log
    ADD CONSTRAINT batch_run_log_pkey PRIMARY KEY (id);


--
-- TOC entry 4830 (class 2606 OID 186119)
-- Name: batch_run batch_run_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.batch_run
    ADD CONSTRAINT batch_run_pkey PRIMARY KEY (id);


--
-- TOC entry 4847 (class 2606 OID 241458)
-- Name: best_idea best_idea_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.best_idea
    ADD CONSTRAINT best_idea_pkey PRIMARY KEY (provider_etf_id, symbol, value_date);


--
-- TOC entry 4854 (class 2606 OID 250208)
-- Name: categorize_etf_holding categorize_etf_holding_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.categorize_etf_holding
    ADD CONSTRAINT categorize_etf_holding_pkey PRIMARY KEY (id);


--
-- TOC entry 4841 (class 2606 OID 239108)
-- Name: categorize_etf categorize_etf_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.categorize_etf
    ADD CONSTRAINT categorize_etf_pkey PRIMARY KEY (id);


--
-- TOC entry 4860 (class 2606 OID 251117)
-- Name: fund_holding_change fund_holding_change_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.fund_holding_change
    ADD CONSTRAINT fund_holding_change_pkey PRIMARY KEY (fund_id, change_date, symbol, direction);


--
-- TOC entry 4858 (class 2606 OID 251100)
-- Name: fund_holding fund_holding_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.fund_holding
    ADD CONSTRAINT fund_holding_pkey PRIMARY KEY (fund_id, holding_date, symbol);


--
-- TOC entry 4852 (class 2606 OID 241486)
-- Name: fund fund_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.fund
    ADD CONSTRAINT fund_pkey PRIMARY KEY (id);


--
-- TOC entry 4828 (class 2606 OID 186112)
-- Name: log log_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.log
    ADD CONSTRAINT log_pkey PRIMARY KEY (id);


--
-- TOC entry 4862 (class 2606 OID 251135)
-- Name: categorize_ticker pk_categorize_ticker; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.categorize_ticker
    ADD CONSTRAINT pk_categorize_ticker PRIMARY KEY (symbol);


--
-- TOC entry 4843 (class 2606 OID 239153)
-- Name: ticker pk_ticker; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.ticker
    ADD CONSTRAINT pk_ticker PRIMARY KEY (symbol);


--
-- TOC entry 4845 (class 2606 OID 239165)
-- Name: ticker_value pk_ticker_value; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.ticker_value
    ADD CONSTRAINT pk_ticker_value PRIMARY KEY (symbol, value_date);


--
-- TOC entry 4839 (class 2606 OID 192301)
-- Name: provider_etf_holding provider_etf_holding_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.provider_etf_holding
    ADD CONSTRAINT provider_etf_holding_pkey PRIMARY KEY (id);


--
-- TOC entry 4835 (class 2606 OID 192270)
-- Name: provider_etf provider_etf_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.provider_etf
    ADD CONSTRAINT provider_etf_pkey PRIMARY KEY (id);


--
-- TOC entry 4826 (class 2606 OID 186070)
-- Name: provider provider_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.provider
    ADD CONSTRAINT provider_pkey PRIMARY KEY (id);


--
-- TOC entry 4848 (class 1259 OID 241469)
-- Name: fki_fk_best_idea_provider_etf_id; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX fki_fk_best_idea_provider_etf_id ON public.best_idea USING btree (provider_etf_id) WITH (fillfactor='100', deduplicate_items='true');


--
-- TOC entry 4849 (class 1259 OID 241470)
-- Name: fki_fk_best_idea_symbol; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX fki_fk_best_idea_symbol ON public.best_idea USING btree (symbol) WITH (fillfactor='100', deduplicate_items='true');


--
-- TOC entry 4850 (class 1259 OID 241471)
-- Name: fki_fk_best_idea_value_date; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX fki_fk_best_idea_value_date ON public.best_idea USING btree (value_date) WITH (fillfactor='100', deduplicate_items='true');


--
-- TOC entry 4855 (class 1259 OID 250214)
-- Name: fki_fk_categorize_etf_holding_categorize_etf_id; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX fki_fk_categorize_etf_holding_categorize_etf_id ON public.categorize_etf_holding USING btree (categorize_etf_id) WITH (fillfactor='100', deduplicate_items='true');


--
-- TOC entry 4836 (class 1259 OID 192307)
-- Name: fki_fk_provider_etf_holding_provider_etf_id; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX fki_fk_provider_etf_holding_provider_etf_id ON public.provider_etf_holding USING btree (provider_etf_id) WITH (fillfactor='100', deduplicate_items='true');


--
-- TOC entry 4833 (class 1259 OID 192276)
-- Name: fki_fk_provider_etf_provider_id; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX fki_fk_provider_etf_provider_id ON public.provider_etf USING btree (provider_id) WITH (fillfactor='100', deduplicate_items='true');


--
-- TOC entry 4856 (class 1259 OID 250215)
-- Name: idx_categorize_etf_holding_trade_date; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX idx_categorize_etf_holding_trade_date ON public.categorize_etf_holding USING btree (holding_date) WITH (fillfactor='100', deduplicate_items='true');


--
-- TOC entry 4837 (class 1259 OID 192308)
-- Name: idx_provider_etf_holding_holding_date; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX idx_provider_etf_holding_holding_date ON public.provider_etf_holding USING btree (holding_date) WITH (fillfactor='100', deduplicate_items='true');


--
-- TOC entry 4866 (class 2606 OID 241459)
-- Name: best_idea fk_best_idea_provider_etf_id; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.best_idea
    ADD CONSTRAINT fk_best_idea_provider_etf_id FOREIGN KEY (provider_etf_id) REFERENCES public.provider_etf(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4867 (class 2606 OID 241464)
-- Name: best_idea fk_best_idea_symbol; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.best_idea
    ADD CONSTRAINT fk_best_idea_symbol FOREIGN KEY (symbol) REFERENCES public.ticker(symbol) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4868 (class 2606 OID 250209)
-- Name: categorize_etf_holding fk_categorize_etf_holding_categorize_etf_id; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.categorize_etf_holding
    ADD CONSTRAINT fk_categorize_etf_holding_categorize_etf_id FOREIGN KEY (categorize_etf_id) REFERENCES public.categorize_etf(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4871 (class 2606 OID 251118)
-- Name: fund_holding_change fk_fund_holding_change_fund_id; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.fund_holding_change
    ADD CONSTRAINT fk_fund_holding_change_fund_id FOREIGN KEY (fund_id) REFERENCES public.fund(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4872 (class 2606 OID 251123)
-- Name: fund_holding_change fk_fund_holding_change_symbol; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.fund_holding_change
    ADD CONSTRAINT fk_fund_holding_change_symbol FOREIGN KEY (symbol) REFERENCES public.ticker(symbol) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4869 (class 2606 OID 251101)
-- Name: fund_holding fk_fund_holding_fund_id; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.fund_holding
    ADD CONSTRAINT fk_fund_holding_fund_id FOREIGN KEY (fund_id) REFERENCES public.fund(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4870 (class 2606 OID 251106)
-- Name: fund_holding fk_fund_holding_symbol; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.fund_holding
    ADD CONSTRAINT fk_fund_holding_symbol FOREIGN KEY (symbol) REFERENCES public.ticker(symbol) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4864 (class 2606 OID 192302)
-- Name: provider_etf_holding fk_provider_etf_holding_provider_etf_id; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.provider_etf_holding
    ADD CONSTRAINT fk_provider_etf_holding_provider_etf_id FOREIGN KEY (provider_etf_id) REFERENCES public.provider_etf(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4863 (class 2606 OID 192271)
-- Name: provider_etf fk_provider_etf_provider_id; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.provider_etf
    ADD CONSTRAINT fk_provider_etf_provider_id FOREIGN KEY (provider_id) REFERENCES public.provider(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4865 (class 2606 OID 251169)
-- Name: ticker_value fk_ticker_symbol; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.ticker_value
    ADD CONSTRAINT fk_ticker_symbol FOREIGN KEY (symbol) REFERENCES public.ticker(symbol) ON UPDATE CASCADE ON DELETE CASCADE;


-- Completed on 2026-04-11 20:05:35

--
-- PostgreSQL database dump complete
--

\unrestrict 9stkKcboB9lVQxEmetJv1KZXtlCFKFFbqIFZlHxWzzsvBc89LLOhv1pqkBTWEM0

