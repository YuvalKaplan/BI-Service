--
-- PostgreSQL database dump
--

\restrict x9W3lV6HdmeMcwwAfN9bl39ulghllhjYAfyIU4nrQjuqeWrsVBLttJTBXMdLVZS

-- Dumped from database version 17.6
-- Dumped by pg_dump version 18.0

-- Started on 2026-04-21 18:39:37

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
-- TOC entry 257 (class 1255 OID 251602)
-- Name: get_best_ideas_by_ranking(integer, text, text, date, integer[], text[], boolean); Type: FUNCTION; Schema: public; Owner: admin
--

CREATE FUNCTION public.get_best_ideas_by_ranking(p_ranking_level integer, p_style_type text, p_cap_type text, p_as_of_date date, p_provider_etf_ids integer[], p_exchanges text[], p_esg_only boolean, p_country_type text DEFAULT 'all') RETURNS TABLE(ticker_id integer, ranking integer, appearances bigint, max_delta double precision, source_etf_id integer, all_provider_ids integer[])
    LANGUAGE sql
    AS $$

    WITH constants AS (
        SELECT INTERVAL '10 days' AS lookback
    ),

    eligible_tickers AS (
        SELECT DISTINCT ON (t.id)
            t.id AS ticker_id,
            (CASE WHEN tv.market_cap >= 10000000000 THEN 'large' ELSE 'mid_small' END) AS calc_cap
        FROM public.ticker t
        JOIN public.ticker_value tv ON t.id = tv.ticker_id,
        constants
        WHERE tv.value_date <= p_as_of_date
          AND tv.value_date >= p_as_of_date - lookback
          AND (p_style_type = 'blend' OR t.style_type = p_style_type)
          AND (cardinality(p_exchanges) = 0 OR t.exchange = ANY(p_exchanges))
          AND (NOT p_esg_only OR t.esg_qualified = TRUE)
          AND (
              p_country_type = 'all'
              OR (p_country_type = 'US'            AND t.country = 'US')
              OR (p_country_type = 'Non-US'        AND t.country IS NOT NULL
                                                   AND t.country <> 'US')
          )
        ORDER BY t.id, tv.value_date DESC
    ),

    filtered_universe AS (
        SELECT ticker_id
        FROM eligible_tickers
        WHERE (p_cap_type = 'all_cap' OR calc_cap = p_cap_type)
    ),

    symbol_latest_date AS (
        SELECT
            be.ticker_id,
            MAX(be.value_date) AS max_date
        FROM public.best_idea be
        JOIN filtered_universe fu ON be.ticker_id = fu.ticker_id,
        constants
        WHERE be.value_date <= p_as_of_date
          AND be.value_date >= p_as_of_date - lookback
          AND (cardinality(p_provider_etf_ids) = 0 OR be.provider_etf_id = ANY(p_provider_etf_ids))
        GROUP BY be.ticker_id
    ),

    symbol_targets AS (
        SELECT DISTINCT ON (be.ticker_id)
            be.ticker_id,
            be.value_date AS max_date,
            be.ranking    AS best_ranking
        FROM public.best_idea be
        JOIN symbol_latest_date sld ON be.ticker_id = sld.ticker_id
                                   AND be.value_date = sld.max_date
        WHERE be.ranking <= p_ranking_level
          AND (cardinality(p_provider_etf_ids) = 0 OR be.provider_etf_id = ANY(p_provider_etf_ids))
        ORDER BY be.ticker_id, be.ranking ASC
    )

    SELECT
        st.ticker_id,
        st.best_ranking                                                AS ranking,
        COUNT(DISTINCT be.provider_etf_id)                            AS appearances,
        MAX(be.delta)                                                  AS max_delta,
        (array_agg(be.provider_etf_id ORDER BY be.delta DESC))[1]    AS source_etf_id,
        array_agg(DISTINCT be.provider_etf_id)                       AS all_provider_ids
    FROM public.best_idea be
    JOIN symbol_targets st ON be.ticker_id = st.ticker_id
                           AND be.value_date = st.max_date
                           AND be.ranking    = st.best_ranking
    JOIN filtered_universe fu ON be.ticker_id = fu.ticker_id
    GROUP BY st.ticker_id, st.best_ranking
    ORDER BY st.best_ranking, appearances DESC, max_delta DESC;

$$;


ALTER FUNCTION public.get_best_ideas_by_ranking(p_ranking_level integer, p_style_type text, p_cap_type text, p_as_of_date date, p_provider_etf_ids integer[], p_exchanges text[], p_esg_only boolean, p_country_type text) OWNER TO admin;

--
-- TOC entry 255 (class 1255 OID 251337)
-- Name: sanitize_tickers(); Type: FUNCTION; Schema: public; Owner: admin
--

CREATE FUNCTION public.sanitize_tickers() RETURNS void
    LANGUAGE sql
    AS $_$
    WITH candidates AS (
        SELECT symbol,
               CASE
                   WHEN symbol !~ '^[A-Z0-9]{1,10}$'  THEN 'Invalid ticker format'
                   WHEN name ~* '\m(ETF|fund)\M'   THEN 'Fund or ETF'
                   WHEN symbol = ANY(ARRAY[
                       'XTSLA','AGPXX','BOXX','CMQXX','DTRXX','FGXXX',
                       'FTIXX','GVMXX','JIMXX','JTSXX','MGMXX','PGLBB','SALXX'
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
$_$;


ALTER FUNCTION public.sanitize_tickers() OWNER TO admin;

--
-- TOC entry 254 (class 1255 OID 251137)
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
		public.categorize_etf_holding,
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

--
-- TOC entry 256 (class 1255 OID 251431)
-- Name: x_m_resequence_ids(); Type: PROCEDURE; Schema: public; Owner: admin
--

CREATE PROCEDURE public.x_m_resequence_ids()
    LANGUAGE sql
    AS $$

-- https://stackoverflow.com/questions/4448340/postgresql-duplicate-key-violates-unique-constraint

SELECT SETVAL((SELECT PG_GET_SERIAL_SEQUENCE('"batch_run"', 'id')), (SELECT (MAX("id") + 1) FROM "batch_run"), FALSE);
SELECT SETVAL((SELECT PG_GET_SERIAL_SEQUENCE('"batch_run_log"', 'id')), (SELECT (MAX("id") + 1) FROM "batch_run_log"), FALSE);
SELECT SETVAL((SELECT PG_GET_SERIAL_SEQUENCE('"categorize_etf"', 'id')), (SELECT (MAX("id") + 1) FROM "categorize_etf"), FALSE);
SELECT SETVAL((SELECT PG_GET_SERIAL_SEQUENCE('"categorize_etf_holding"', 'id')), (SELECT (MAX("id") + 1) FROM "categorize_etf_holding"), FALSE);
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
    ticker_id integer NOT NULL,
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
-- TOC entry 237 (class 1259 OID 250201)
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
-- Name: categorize_ticker; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.categorize_ticker (
    id          integer GENERATED ALWAYS AS IDENTITY NOT NULL,
    name        text,
    symbol      character varying(32) NOT NULL,
    isin        character varying(16),
    exchange    character varying(32),
    country     character varying(8),
    currency    character varying(8),
    style_type  character varying(16),
    cap_type    character varying(16),
    sector      text,
    market_cap  bigint,
    factors     jsonb,
    last_update timestamp without time zone,
    CONSTRAINT categorize_ticker_pkey PRIMARY KEY (id),
    CONSTRAINT categorize_ticker_symbol_exchange_key UNIQUE (symbol, exchange)
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
    ticker_id integer NOT NULL,
    holding_date date NOT NULL,
    ranking integer NOT NULL,
    source_etf_id integer,
    max_delta double precision,
    weight double precision
);


ALTER TABLE public.fund_holding OWNER TO admin;

--
-- TOC entry 239 (class 1259 OID 251111)
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
    ticker_id integer,
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
    id integer NOT NULL,
    created_at timestamp without time zone DEFAULT (now() AT TIME ZONE 'utc'::text) NOT NULL,
    source character varying(16),
    style_type character varying(16),
    cap_type character varying(16),
    type_from character varying(16),
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
    invalid text
);


ALTER TABLE public.ticker OWNER TO admin;

--
-- TOC entry 240 (class 1259 OID 251524)
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
-- TOC entry 232 (class 1259 OID 239154)
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
-- TOC entry 4829 (class 2606 OID 186137)
-- Name: batch_run_log batch_run_log_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.batch_run_log
    ADD CONSTRAINT batch_run_log_pkey PRIMARY KEY (id);


--
-- TOC entry 4827 (class 2606 OID 186119)
-- Name: batch_run batch_run_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.batch_run
    ADD CONSTRAINT batch_run_pkey PRIMARY KEY (id);


--
-- TOC entry 4848 (class 2606 OID 251596)
-- Name: best_idea best_idea_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.best_idea
    ADD CONSTRAINT best_idea_pkey PRIMARY KEY (provider_etf_id, ticker_id, value_date);


--
-- TOC entry 4854 (class 2606 OID 250208)
-- Name: categorize_etf_holding categorize_etf_holding_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.categorize_etf_holding
    ADD CONSTRAINT categorize_etf_holding_pkey PRIMARY KEY (id);


--
-- TOC entry 4839 (class 2606 OID 239108)
-- Name: categorize_etf categorize_etf_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.categorize_etf
    ADD CONSTRAINT categorize_etf_pkey PRIMARY KEY (id);


--
-- TOC entry 4863 (class 2606 OID 251600)
-- Name: fund_holding_change fund_holding_change_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.fund_holding_change
    ADD CONSTRAINT fund_holding_change_pkey PRIMARY KEY (fund_id, change_date, ticker_id, direction);


--
-- TOC entry 4860 (class 2606 OID 251598)
-- Name: fund_holding fund_holding_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.fund_holding
    ADD CONSTRAINT fund_holding_pkey PRIMARY KEY (fund_id, holding_date, ticker_id);


--
-- TOC entry 4852 (class 2606 OID 241486)
-- Name: fund fund_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.fund
    ADD CONSTRAINT fund_pkey PRIMARY KEY (id);


--
-- TOC entry 4825 (class 2606 OID 186112)
-- Name: log log_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.log
    ADD CONSTRAINT log_pkey PRIMARY KEY (id);


--
-- TOC entry 4846 (class 2606 OID 251594)
-- Name: ticker_value pk_ticker_value; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.ticker_value
    ADD CONSTRAINT pk_ticker_value PRIMARY KEY (ticker_id, value_date);


--
-- TOC entry 4837 (class 2606 OID 192301)
-- Name: provider_etf_holding provider_etf_holding_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.provider_etf_holding
    ADD CONSTRAINT provider_etf_holding_pkey PRIMARY KEY (id);


--
-- TOC entry 4832 (class 2606 OID 192270)
-- Name: provider_etf provider_etf_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.provider_etf
    ADD CONSTRAINT provider_etf_pkey PRIMARY KEY (id);


--
-- TOC entry 4823 (class 2606 OID 186070)
-- Name: provider provider_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.provider
    ADD CONSTRAINT provider_pkey PRIMARY KEY (id);


--
-- TOC entry 4841 (class 2606 OID 251534)
-- Name: ticker ticker_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.ticker
    ADD CONSTRAINT ticker_pkey PRIMARY KEY (id);


--
-- TOC entry 4843 (class 2606 OID 251605)
-- Name: ticker ticker_symbol_key; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.ticker
    ADD CONSTRAINT ticker_symbol_key UNIQUE (symbol);


--
-- TOC entry 4849 (class 1259 OID 241469)
-- Name: fki_fk_best_idea_provider_etf_id; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX fki_fk_best_idea_provider_etf_id ON public.best_idea USING btree (provider_etf_id) WITH (fillfactor='100', deduplicate_items='true');


--
-- TOC entry 4850 (class 1259 OID 251548)
-- Name: fki_fk_best_idea_ticker_id; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX fki_fk_best_idea_ticker_id ON public.best_idea USING btree (ticker_id) WITH (fillfactor='100', deduplicate_items='true');


--
-- TOC entry 4855 (class 1259 OID 250214)
-- Name: fki_fk_categorize_etf_holding_categorize_etf_id; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX fki_fk_categorize_etf_holding_categorize_etf_id ON public.categorize_etf_holding USING btree (categorize_etf_id) WITH (fillfactor='100', deduplicate_items='true');


--
-- TOC entry 4856 (class 1259 OID 251584)
-- Name: fki_fk_categorize_etf_holding_categorize_ticker_id; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX fki_fk_categorize_etf_holding_categorize_ticker_id ON public.categorize_etf_holding USING btree (categorize_ticker_id) WITH (fillfactor='100', deduplicate_items='true');


--
-- TOC entry 4861 (class 1259 OID 251564)
-- Name: fki_fk_fund_holding_change_ticker_id; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX fki_fk_fund_holding_change_ticker_id ON public.fund_holding_change USING btree (ticker_id) WITH (fillfactor='100', deduplicate_items='true');


--
-- TOC entry 4858 (class 1259 OID 251556)
-- Name: fki_fk_fund_holding_ticker_id; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX fki_fk_fund_holding_ticker_id ON public.fund_holding USING btree (ticker_id) WITH (fillfactor='100', deduplicate_items='true');


--
-- TOC entry 4833 (class 1259 OID 192307)
-- Name: fki_fk_provider_etf_holding_provider_etf_id; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX fki_fk_provider_etf_holding_provider_etf_id ON public.provider_etf_holding USING btree (provider_etf_id) WITH (fillfactor='100', deduplicate_items='true');


--
-- TOC entry 4834 (class 1259 OID 251578)
-- Name: fki_fk_provider_etf_holding_ticker_id; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX fki_fk_provider_etf_holding_ticker_id ON public.provider_etf_holding USING btree (ticker_id) WITH (fillfactor='100', deduplicate_items='true');


--
-- TOC entry 4830 (class 1259 OID 192276)
-- Name: fki_fk_provider_etf_provider_id; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX fki_fk_provider_etf_provider_id ON public.provider_etf USING btree (provider_id) WITH (fillfactor='100', deduplicate_items='true');


--
-- TOC entry 4844 (class 1259 OID 251540)
-- Name: fki_fk_ticker_value_ticker_id; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX fki_fk_ticker_value_ticker_id ON public.ticker_value USING btree (ticker_id) WITH (fillfactor='100', deduplicate_items='true');


--
-- TOC entry 4857 (class 1259 OID 250215)
-- Name: idx_categorize_etf_holding_trade_date; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX idx_categorize_etf_holding_trade_date ON public.categorize_etf_holding USING btree (holding_date) WITH (fillfactor='100', deduplicate_items='true');


--
-- TOC entry 4835 (class 1259 OID 192308)
-- Name: idx_provider_etf_holding_holding_date; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX idx_provider_etf_holding_holding_date ON public.provider_etf_holding USING btree (holding_date) WITH (fillfactor='100', deduplicate_items='true');


--
-- TOC entry 4868 (class 2606 OID 241459)
-- Name: best_idea fk_best_idea_provider_etf_id; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.best_idea
    ADD CONSTRAINT fk_best_idea_provider_etf_id FOREIGN KEY (provider_etf_id) REFERENCES public.provider_etf(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4869 (class 2606 OID 251543)
-- Name: best_idea fk_best_idea_ticker_id; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.best_idea
    ADD CONSTRAINT fk_best_idea_ticker_id FOREIGN KEY (ticker_id) REFERENCES public.ticker(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4870 (class 2606 OID 250209)
-- Name: categorize_etf_holding fk_categorize_etf_holding_categorize_etf_id; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.categorize_etf_holding
    ADD CONSTRAINT fk_categorize_etf_holding_categorize_etf_id FOREIGN KEY (categorize_etf_id) REFERENCES public.categorize_etf(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- Name: categorize_etf_holding fk_categorize_etf_holding_categorize_ticker_id; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.categorize_etf_holding
    ADD CONSTRAINT fk_categorize_etf_holding_categorize_ticker_id FOREIGN KEY (categorize_ticker_id) REFERENCES public.categorize_ticker(id) ON UPDATE CASCADE ON DELETE SET NULL;


--
-- TOC entry 4874 (class 2606 OID 251118)
-- Name: fund_holding_change fk_fund_holding_change_fund_id; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.fund_holding_change
    ADD CONSTRAINT fk_fund_holding_change_fund_id FOREIGN KEY (fund_id) REFERENCES public.fund(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4875 (class 2606 OID 251559)
-- Name: fund_holding_change fk_fund_holding_change_ticker_id; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.fund_holding_change
    ADD CONSTRAINT fk_fund_holding_change_ticker_id FOREIGN KEY (ticker_id) REFERENCES public.ticker(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4872 (class 2606 OID 251101)
-- Name: fund_holding fk_fund_holding_fund_id; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.fund_holding
    ADD CONSTRAINT fk_fund_holding_fund_id FOREIGN KEY (fund_id) REFERENCES public.fund(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4873 (class 2606 OID 251551)
-- Name: fund_holding fk_fund_holding_ticker_id; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.fund_holding
    ADD CONSTRAINT fk_fund_holding_ticker_id FOREIGN KEY (ticker_id) REFERENCES public.ticker(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4865 (class 2606 OID 192302)
-- Name: provider_etf_holding fk_provider_etf_holding_provider_etf_id; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.provider_etf_holding
    ADD CONSTRAINT fk_provider_etf_holding_provider_etf_id FOREIGN KEY (provider_etf_id) REFERENCES public.provider_etf(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4866 (class 2606 OID 251573)
-- Name: provider_etf_holding fk_provider_etf_holding_ticker_id; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.provider_etf_holding
    ADD CONSTRAINT fk_provider_etf_holding_ticker_id FOREIGN KEY (ticker_id) REFERENCES public.ticker(id) ON UPDATE CASCADE ON DELETE SET NULL;


--
-- TOC entry 4864 (class 2606 OID 192271)
-- Name: provider_etf fk_provider_etf_provider_id; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.provider_etf
    ADD CONSTRAINT fk_provider_etf_provider_id FOREIGN KEY (provider_id) REFERENCES public.provider(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4867 (class 2606 OID 251535)
-- Name: ticker_value fk_ticker_value_ticker_id; Type: FK CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.ticker_value
    ADD CONSTRAINT fk_ticker_value_ticker_id FOREIGN KEY (ticker_id) REFERENCES public.ticker(id) ON UPDATE CASCADE ON DELETE CASCADE;


-- Completed on 2026-04-21 18:39:37

--
-- PostgreSQL database dump complete
--

\unrestrict x9W3lV6HdmeMcwwAfN9bl39ulghllhjYAfyIU4nrQjuqeWrsVBLttJTBXMdLVZS

