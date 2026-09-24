--
-- PostgreSQL database dump
--

\restrict wPqYW2v6yuPdECScu8tcsl1OwpXcsUBTfbxLbApQXa1z6udgRVxPeCGS23MOyOX

-- Dumped from database version 18.3
-- Dumped by pg_dump version 18.2

-- Started on 2026-09-23 17:53:19

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
-- TOC entry 249 (class 1255 OID 16390)
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
		public.benchmark,
		public.benchmark_holding,
		public.best_idea, 
		public.categorize_etf,
		public.categorize_etf_holding,
		public.categorize_ticker,
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
-- TOC entry 261 (class 1255 OID 16391)
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

--
-- TOC entry 262 (class 1255 OID 73899)
-- Name: x_m_resequence_ids(); Type: PROCEDURE; Schema: public; Owner: admin
--

CREATE PROCEDURE public.x_m_resequence_ids()
    LANGUAGE sql
    AS $$

-- https://stackoverflow.com/questions/4448340/postgresql-duplicate-key-violates-unique-constraint

SELECT SETVAL((SELECT PG_GET_SERIAL_SEQUENCE('"batch_run"', 'id')), (SELECT (MAX("id") + 1) FROM "batch_run"), FALSE);
SELECT SETVAL((SELECT PG_GET_SERIAL_SEQUENCE('"batch_run_log"', 'id')), (SELECT (MAX("id") + 1) FROM "batch_run_log"), FALSE);
SELECT SETVAL((SELECT PG_GET_SERIAL_SEQUENCE('"benchmark"', 'id')), (SELECT (MAX("id") + 1) FROM "benchmark"), FALSE);
SELECT SETVAL((SELECT PG_GET_SERIAL_SEQUENCE('"benchmark_holding"', 'id')), (SELECT (MAX("id") + 1) FROM "benchmark_holding"), FALSE);
SELECT SETVAL((SELECT PG_GET_SERIAL_SEQUENCE('"categorize_etf"', 'id')), (SELECT (MAX("id") + 1) FROM "categorize_etf"), FALSE);
SELECT SETVAL((SELECT PG_GET_SERIAL_SEQUENCE('"categorize_etf_holding"', 'id')), (SELECT (MAX("id") + 1) FROM "categorize_etf_holding"), FALSE);
SELECT SETVAL((SELECT PG_GET_SERIAL_SEQUENCE('"categorize_ticker"', 'id')), (SELECT (MAX("id") + 1) FROM "categorize_ticker"), FALSE);
SELECT SETVAL((SELECT PG_GET_SERIAL_SEQUENCE('"fund"', 'id')), (SELECT (MAX("id") + 1) FROM "fund"), FALSE);
SELECT SETVAL((SELECT PG_GET_SERIAL_SEQUENCE('"log"', 'id')), (SELECT (MAX("id") + 1) FROM "log"), FALSE);
SELECT SETVAL((SELECT PG_GET_SERIAL_SEQUENCE('"provider"', 'id')), (SELECT (MAX("id") + 1) FROM "provider"), FALSE);
SELECT SETVAL((SELECT PG_GET_SERIAL_SEQUENCE('"provider_etf"', 'id')), (SELECT (MAX("id") + 1) FROM "provider_etf"), FALSE);
SELECT SETVAL((SELECT PG_GET_SERIAL_SEQUENCE('"provider_etf_holding"', 'id')), (SELECT (MAX("id") + 1) FROM "provider_etf_holding"), FALSE);

$$;


ALTER PROCEDURE public.x_m_resequence_ids() OWNER TO admin;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 219 (class 1259 OID 16393)
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
-- TOC entry 220 (class 1259 OID 16399)
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
-- TOC entry 221 (class 1259 OID 16400)
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
-- TOC entry 222 (class 1259 OID 16409)
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
-- TOC entry 246 (class 1259 OID 73843)
-- Name: benchmark; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.benchmark (
    id integer NOT NULL,
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    name text NOT NULL,
    region text NOT NULL,
    cap_type text NOT NULL,
    style_type text NOT NULL,
    market_cap_min bigint NOT NULL,
    disabled boolean DEFAULT false NOT NULL
);


ALTER TABLE public.benchmark OWNER TO admin;

--
-- TOC entry 248 (class 1259 OID 73862)
-- Name: benchmark_holding; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.benchmark_holding (
    id integer NOT NULL,
    benchmark_id integer NOT NULL,
    holding_date date NOT NULL,
    ticker_id integer NOT NULL,
    market_cap double precision NOT NULL,
    weight double precision NOT NULL
);


ALTER TABLE public.benchmark_holding OWNER TO admin;

--
-- TOC entry 247 (class 1259 OID 73861)
-- Name: benchmark_holding_id_seq; Type: SEQUENCE; Schema: public; Owner: admin
--

CREATE SEQUENCE public.benchmark_holding_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.benchmark_holding_id_seq OWNER TO admin;

--
-- TOC entry 5176 (class 0 OID 0)
-- Dependencies: 247
-- Name: benchmark_holding_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: admin
--

ALTER SEQUENCE public.benchmark_holding_id_seq OWNED BY public.benchmark_holding.id;


--
-- TOC entry 245 (class 1259 OID 73842)
-- Name: benchmark_id_seq; Type: SEQUENCE; Schema: public; Owner: admin
--

CREATE SEQUENCE public.benchmark_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.benchmark_id_seq OWNER TO admin;

--
-- TOC entry 5177 (class 0 OID 0)
-- Dependencies: 245
-- Name: benchmark_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: admin
--

ALTER SEQUENCE public.benchmark_id_seq OWNED BY public.benchmark.id;


--
-- TOC entry 223 (class 1259 OID 16410)
-- Name: best_idea; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.best_idea (
    provider_etf_id integer NOT NULL,
    ticker_id integer NOT NULL,
    value_date date NOT NULL,
    etf_weight double precision,
    benchmark_weight double precision,
    delta double precision,
    ranking integer,
    benchmark_mode text DEFAULT 'self'::text NOT NULL
);


ALTER TABLE public.best_idea OWNER TO admin;

--
-- TOC entry 224 (class 1259 OID 16416)
-- Name: categorize_etf; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.categorize_etf (
    id integer NOT NULL,
    created_at timestamp without time zone DEFAULT (now() AT TIME ZONE 'utc'::text) NOT NULL,
    name text,
    region character varying(8),
    usage character varying(10),
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
-- TOC entry 225 (class 1259 OID 16425)
-- Name: categorize_etf_holding; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.categorize_etf_holding (
    id integer NOT NULL,
    created_at timestamp without time zone DEFAULT (now() AT TIME ZONE 'utc'::text) NOT NULL,
    categorize_etf_id integer NOT NULL,
    holding_date timestamp without time zone NOT NULL,
    categorize_ticker_id integer
);


ALTER TABLE public.categorize_etf_holding OWNER TO admin;

--
-- TOC entry 226 (class 1259 OID 16433)
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
-- TOC entry 227 (class 1259 OID 16434)
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
-- TOC entry 228 (class 1259 OID 16435)
-- Name: categorize_ticker; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.categorize_ticker (
    id integer NOT NULL,
    name text,
    symbol character varying(32) NOT NULL,
    isin character varying(16),
    exchange character varying(32),
    country character varying(8),
    currency character varying(8),
    style_type character varying(16),
    cap_type character varying(16),
    sector text,
    market_cap bigint,
    factors jsonb,
    last_update timestamp without time zone
);


ALTER TABLE public.categorize_ticker OWNER TO admin;

--
-- TOC entry 229 (class 1259 OID 16442)
-- Name: categorize_ticker_id_seq; Type: SEQUENCE; Schema: public; Owner: admin
--

ALTER TABLE public.categorize_ticker ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.categorize_ticker_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 230 (class 1259 OID 16443)
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
-- TOC entry 231 (class 1259 OID 16453)
-- Name: fund_holding; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.fund_holding (
    fund_id integer NOT NULL,
    ticker_id integer NOT NULL,
    holding_date date NOT NULL,
    ranking integer NOT NULL,
    source_etf_id integer,
    max_delta double precision,
    weight double precision
);


ALTER TABLE public.fund_holding OWNER TO admin;

--
-- TOC entry 232 (class 1259 OID 16460)
-- Name: fund_holding_change; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.fund_holding_change (
    fund_id integer NOT NULL,
    ticker_id integer NOT NULL,
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
-- TOC entry 233 (class 1259 OID 16469)
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
-- TOC entry 234 (class 1259 OID 16470)
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
-- TOC entry 235 (class 1259 OID 16478)
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
-- TOC entry 236 (class 1259 OID 16479)
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
-- TOC entry 237 (class 1259 OID 16489)
-- Name: provider_etf; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.provider_etf (
    id integer NOT NULL,
    created_at timestamp without time zone DEFAULT (now() AT TIME ZONE 'utc'::text) NOT NULL,
    provider_id integer NOT NULL,
    disabled boolean DEFAULT false NOT NULL,
    disabled_reason text,
    region text NOT NULL,
    name text,
    description text,
    isin text,
    ticker text,
    cap_type text,
    style_type text,
    benchmark text,
    trading_since timestamp without time zone,
    number_of_managers integer,
    url text,
    wait_pre_events text,
    wait_post_events text,
    events jsonb,
    trigger_download jsonb,
    mapping jsonb,
    file_format character varying(10),
    last_downloaded timestamp without time zone,
    benchmark_id integer
);


ALTER TABLE public.provider_etf OWNER TO admin;

--
-- TOC entry 238 (class 1259 OID 16501)
-- Name: provider_etf_holding; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.provider_etf_holding (
    id integer NOT NULL,
    created_at timestamp without time zone DEFAULT (now() AT TIME ZONE 'utc'::text) NOT NULL,
    provider_etf_id integer NOT NULL,
    holding_date timestamp without time zone NOT NULL,
    ticker_id integer,
    shares double precision,
    market_value double precision,
    weight double precision
);


ALTER TABLE public.provider_etf_holding OWNER TO admin;

--
-- TOC entry 239 (class 1259 OID 16509)
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
-- TOC entry 240 (class 1259 OID 16510)
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
-- TOC entry 241 (class 1259 OID 16511)
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
-- TOC entry 242 (class 1259 OID 16512)
-- Name: ticker; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.ticker (
    id integer NOT NULL,
    created_at timestamp without time zone DEFAULT (now() AT TIME ZONE 'utc'::text) NOT NULL,
    updated_at timestamp without time zone,
    source character varying(16),
    style_type character varying(16),
    cap_type character varying(16),
    type_from character varying(16),
    style_factors_failed_at timestamp without time zone,
    symbol character varying(32) NOT NULL,
    isin character varying(16),
    cusip character varying(16),
    cik character varying(16),
    exchange character varying(32),
    name text,
    industry text,
    sector text,
    country character varying(8),
    currency character varying(8),
    esg_factors jsonb,
    esg_qualified boolean,
    is_actively_trading boolean,
    master_ticker_id integer,
    accumulated_market_cap double precision,
    invalid text,
    CONSTRAINT ticker_master_ticker_id_not_self CHECK (((master_ticker_id IS NULL) OR (master_ticker_id <> id)))
);


ALTER TABLE public.ticker OWNER TO admin;

--
-- TOC entry 243 (class 1259 OID 16521)
-- Name: ticker_id_seq; Type: SEQUENCE; Schema: public; Owner: admin
--

ALTER TABLE public.ticker ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.ticker_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 244 (class 1259 OID 16522)
-- Name: ticker_value; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.ticker_value (
    ticker_id integer NOT NULL,
    value_date date NOT NULL,
    stock_price double precision,
    market_cap double precision
);


ALTER TABLE public.ticker_value OWNER TO admin;

--
-- TOC entry 4949 (class 2604 OID 73846)
-- Name: benchmark id; Type: DEFAULT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.benchmark ALTER COLUMN id SET DEFAULT nextval('public.benchmark_id_seq'::regclass);


--
-- TOC entry 4952 (class 2604 OID 73865)
-- Name: benchmark_holding id; Type: DEFAULT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.benchmark_holding ALTER COLUMN id SET DEFAULT nextval('public.benchmark_holding_id_seq'::regclass);


--
-- TOC entry 4957 (class 2606 OID 16546)
-- Name: batch_run_log batch_run_log_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.batch_run_log
    ADD CONSTRAINT batch_run_log_pkey PRIMARY KEY (id);


--
-- TOC entry 4955 (class 2606 OID 16548)
-- Name: batch_run batch_run_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.batch_run
    ADD CONSTRAINT batch_run_pkey PRIMARY KEY (id);


--
-- TOC entry 5004 (class 2606 OID 73875)
-- Name: benchmark_holding benchmark_holding_benchmark_id_holding_date_ticker_id_key; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.benchmark_holding
    ADD CONSTRAINT benchmark_holding_benchmark_id_holding_date_ticker_id_key UNIQUE (benchmark_id, holding_date, ticker_id);


--
-- TOC entry 5006 (class 2606 OID 73873)
-- Name: benchmark_holding benchmark_holding_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.benchmark_holding
    ADD CONSTRAINT benchmark_holding_pkey PRIMARY KEY (id);


--
-- TOC entry 5002 (class 2606 OID 73860)
-- Name: benchmark benchmark_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.benchmark
    ADD CONSTRAINT benchmark_pkey PRIMARY KEY (id);


--
-- TOC entry 4959 (class 2606 OID 73897)
-- Name: best_idea best_idea_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.best_idea
    ADD CONSTRAINT best_idea_pkey PRIMARY KEY (provider_etf_id, ticker_id, value_date, benchmark_mode);


--
-- TOC entry 4965 (class 2606 OID 16552)
-- Name: categorize_etf_holding categorize_etf_holding_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.categorize_etf_holding
    ADD CONSTRAINT categorize_etf_holding_pkey PRIMARY KEY (id);


--
-- TOC entry 4963 (class 2606 OID 16554)
-- Name: categorize_etf categorize_etf_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.categorize_etf
    ADD CONSTRAINT categorize_etf_pkey PRIMARY KEY (id);


--
-- TOC entry 4970 (class 2606 OID 16556)
-- Name: categorize_ticker categorize_ticker_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.categorize_ticker
    ADD CONSTRAINT categorize_ticker_pkey PRIMARY KEY (id);


--
-- TOC entry 4972 (class 2606 OID 16558)
-- Name: categorize_ticker categorize_ticker_symbol_exchange_key; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.categorize_ticker
    ADD CONSTRAINT categorize_ticker_symbol_exchange_key UNIQUE (symbol, exchange);


--
-- TOC entry 4980 (class 2606 OID 16560)
-- Name: fund_holding_change fund_holding_change_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.fund_holding_change
    ADD CONSTRAINT fund_holding_change_pkey PRIMARY KEY (fund_id, change_date, ticker_id, direction);


--
-- TOC entry 4977 (class 2606 OID 16562)
-- Name: fund_holding fund_holding_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.fund_holding
    ADD CONSTRAINT fund_holding_pkey PRIMARY KEY (fund_id, holding_date, ticker_id);


--
-- TOC entry 4974 (class 2606 OID 16564)
-- Name: fund fund_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.fund
    ADD CONSTRAINT fund_pkey PRIMARY KEY (id);


--
-- TOC entry 4982 (class 2606 OID 16566)
-- Name: log log_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.log
    ADD CONSTRAINT log_pkey PRIMARY KEY (id);


--
-- TOC entry 5000 (class 2606 OID 16568)
-- Name: ticker_value pk_ticker_value; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.ticker_value
    ADD CONSTRAINT pk_ticker_value PRIMARY KEY (ticker_id, value_date);


--
-- TOC entry 4992 (class 2606 OID 16570)
-- Name: provider_etf_holding provider_etf_holding_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.provider_etf_holding
    ADD CONSTRAINT provider_etf_holding_pkey PRIMARY KEY (id);


--
-- TOC entry 4987 (class 2606 OID 16572)
-- Name: provider_etf provider_etf_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.provider_etf
    ADD CONSTRAINT provider_etf_pkey PRIMARY KEY (id);


--
-- TOC entry 4984 (class 2606 OID 16574)
-- Name: provider provider_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.provider
    ADD CONSTRAINT provider_pkey PRIMARY KEY (id);


--
-- TOC entry 4995 (class 2606 OID 16576)
-- Name: ticker ticker_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.ticker
    ADD CONSTRAINT ticker_pkey PRIMARY KEY (id);


--
-- TOC entry 4997 (class 2606 OID 74288)
-- Name: ticker ticker_symbol_exchange_key; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.ticker
    ADD CONSTRAINT ticker_symbol_exchange_key UNIQUE (symbol, exchange);


--
-- TOC entry 4960 (class 1259 OID 16579)
-- Name: fki_fk_best_idea_provider_etf_id; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX fki_fk_best_idea_provider_etf_id ON public.best_idea USING btree (provider_etf_id) WITH (fillfactor='100', deduplicate_items='true');


--
-- TOC entry 4961 (class 1259 OID 16580)
-- Name: fki_fk_best_idea_ticker_id; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX fki_fk_best_idea_ticker_id ON public.best_idea USING btree (ticker_id) WITH (fillfactor='100', deduplicate_items='true');


--
-- TOC entry 4966 (class 1259 OID 16581)
-- Name: fki_fk_categorize_etf_holding_categorize_etf_id; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX fki_fk_categorize_etf_holding_categorize_etf_id ON public.categorize_etf_holding USING btree (categorize_etf_id) WITH (fillfactor='100', deduplicate_items='true');


--
-- TOC entry 4967 (class 1259 OID 16582)
-- Name: fki_fk_categorize_etf_holding_ticker_id; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX fki_fk_categorize_etf_holding_ticker_id ON public.categorize_etf_holding USING btree (categorize_ticker_id) WITH (fillfactor='100', deduplicate_items='true');


--
-- TOC entry 4978 (class 1259 OID 16583)
-- Name: fki_fk_fund_holding_change_ticker_id; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX fki_fk_fund_holding_change_ticker_id ON public.fund_holding_change USING btree (ticker_id) WITH (fillfactor='100', deduplicate_items='true');


--
-- TOC entry 4975 (class 1259 OID 16584)
-- Name: fki_fk_fund_holding_ticker_id; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX fki_fk_fund_holding_ticker_id ON public.fund_holding USING btree (ticker_id) WITH (fillfactor='100', deduplicate_items='true');


--
-- TOC entry 4988 (class 1259 OID 16585)
-- Name: fki_fk_provider_etf_holding_provider_etf_id; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX fki_fk_provider_etf_holding_provider_etf_id ON public.provider_etf_holding USING btree (provider_etf_id) WITH (fillfactor='100', deduplicate_items='true');


--
-- TOC entry 4989 (class 1259 OID 16586)
-- Name: fki_fk_provider_etf_holding_ticker_id; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX fki_fk_provider_etf_holding_ticker_id ON public.provider_etf_holding USING btree (ticker_id) WITH (fillfactor='100', deduplicate_items='true');


--
-- TOC entry 4985 (class 1259 OID 16587)
-- Name: fki_fk_provider_etf_provider_id; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX fki_fk_provider_etf_provider_id ON public.provider_etf USING btree (provider_id) WITH (fillfactor='100', deduplicate_items='true');


--
-- TOC entry 4998 (class 1259 OID 16588)
-- Name: fki_fk_ticker_value_ticker_id; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX fki_fk_ticker_value_ticker_id ON public.ticker_value USING btree (ticker_id) WITH (fillfactor='100', deduplicate_items='true');


--
-- TOC entry 5007 (class 1259 OID 73886)
-- Name: idx_benchmark_holding_bid_date; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX idx_benchmark_holding_bid_date ON public.benchmark_holding USING btree (benchmark_id, holding_date);


--
-- TOC entry 4968 (class 1259 OID 16589)
-- Name: idx_categorize_etf_holding_trade_date; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX idx_categorize_etf_holding_trade_date ON public.categorize_etf_holding USING btree (holding_date) WITH (fillfactor='100', deduplicate_items='true');


--
-- TOC entry 4990 (class 1259 OID 16590)
-- Name: idx_provider_etf_holding_holding_date; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX idx_provider_etf_holding_holding_date ON public.provider_etf_holding USING btree (holding_date) WITH (fillfactor='100', deduplicate_items='true');


--
-- TOC entry 4993 (class 1259 OID 74295)
-- Name: idx_ticker_master_ticker_id; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX idx_ticker_master_ticker_id ON public.ticker USING btree (master_ticker_id) WHERE (master_ticker_id IS NOT NULL);


--
-- TOC entry 5022 (class 2606 OID 73876)
-- Name: benchmark_holding benchmark_holding_benchmark_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.benchmark_holding
    ADD CONSTRAINT benchmark_holding_benchmark_id_fkey FOREIGN KEY (benchmark_id) REFERENCES public.benchmark(id);


--
-- TOC entry 5023 (class 2606 OID 73881)
-- Name: benchmark_holding benchmark_holding_ticker_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.benchmark_holding
    ADD CONSTRAINT benchmark_holding_ticker_id_fkey FOREIGN KEY (ticker_id) REFERENCES public.ticker(id);


--
-- TOC entry 5008 (class 2606 OID 16591)
-- Name: best_idea fk_best_idea_provider_etf_id; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.best_idea
    ADD CONSTRAINT fk_best_idea_provider_etf_id FOREIGN KEY (provider_etf_id) REFERENCES public.provider_etf(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 5009 (class 2606 OID 16596)
-- Name: best_idea fk_best_idea_ticker_id; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.best_idea
    ADD CONSTRAINT fk_best_idea_ticker_id FOREIGN KEY (ticker_id) REFERENCES public.ticker(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 5010 (class 2606 OID 16601)
-- Name: categorize_etf_holding fk_categorize_etf_holding_categorize_etf_id; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.categorize_etf_holding
    ADD CONSTRAINT fk_categorize_etf_holding_categorize_etf_id FOREIGN KEY (categorize_etf_id) REFERENCES public.categorize_etf(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 5011 (class 2606 OID 16606)
-- Name: categorize_etf_holding fk_categorize_etf_holding_categorize_ticker_id; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.categorize_etf_holding
    ADD CONSTRAINT fk_categorize_etf_holding_categorize_ticker_id FOREIGN KEY (categorize_ticker_id) REFERENCES public.categorize_ticker(id) ON UPDATE CASCADE ON DELETE SET NULL;


--
-- TOC entry 5014 (class 2606 OID 16611)
-- Name: fund_holding_change fk_fund_holding_change_fund_id; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.fund_holding_change
    ADD CONSTRAINT fk_fund_holding_change_fund_id FOREIGN KEY (fund_id) REFERENCES public.fund(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 5015 (class 2606 OID 16616)
-- Name: fund_holding_change fk_fund_holding_change_ticker_id; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.fund_holding_change
    ADD CONSTRAINT fk_fund_holding_change_ticker_id FOREIGN KEY (ticker_id) REFERENCES public.ticker(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 5012 (class 2606 OID 16621)
-- Name: fund_holding fk_fund_holding_fund_id; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.fund_holding
    ADD CONSTRAINT fk_fund_holding_fund_id FOREIGN KEY (fund_id) REFERENCES public.fund(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 5013 (class 2606 OID 16626)
-- Name: fund_holding fk_fund_holding_ticker_id; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.fund_holding
    ADD CONSTRAINT fk_fund_holding_ticker_id FOREIGN KEY (ticker_id) REFERENCES public.ticker(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 5018 (class 2606 OID 16631)
-- Name: provider_etf_holding fk_provider_etf_holding_provider_etf_id; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.provider_etf_holding
    ADD CONSTRAINT fk_provider_etf_holding_provider_etf_id FOREIGN KEY (provider_etf_id) REFERENCES public.provider_etf(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 5019 (class 2606 OID 16636)
-- Name: provider_etf_holding fk_provider_etf_holding_ticker_id; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.provider_etf_holding
    ADD CONSTRAINT fk_provider_etf_holding_ticker_id FOREIGN KEY (ticker_id) REFERENCES public.ticker(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 5016 (class 2606 OID 16641)
-- Name: provider_etf fk_provider_etf_provider_id; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.provider_etf
    ADD CONSTRAINT fk_provider_etf_provider_id FOREIGN KEY (provider_id) REFERENCES public.provider(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 5021 (class 2606 OID 16646)
-- Name: ticker_value fk_ticker_value_ticker_id; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.ticker_value
    ADD CONSTRAINT fk_ticker_value_ticker_id FOREIGN KEY (ticker_id) REFERENCES public.ticker(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 5017 (class 2606 OID 73887)
-- Name: provider_etf provider_etf_benchmark_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.provider_etf
    ADD CONSTRAINT provider_etf_benchmark_id_fkey FOREIGN KEY (benchmark_id) REFERENCES public.benchmark(id) ON UPDATE CASCADE ON DELETE SET NULL;


--
-- TOC entry 5020 (class 2606 OID 74289)
-- Name: ticker ticker_master_ticker_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.ticker
    ADD CONSTRAINT ticker_master_ticker_id_fkey FOREIGN KEY (master_ticker_id) REFERENCES public.ticker(id);


-- Completed on 2026-09-23 17:53:20

--
-- PostgreSQL database dump complete
--

\unrestrict wPqYW2v6yuPdECScu8tcsl1OwpXcsUBTfbxLbApQXa1z6udgRVxPeCGS23MOyOX

