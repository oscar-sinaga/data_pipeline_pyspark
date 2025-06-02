--
-- PostgreSQL database dump
--

-- Dumped from database version 17.0 (Debian 17.0-1.pgdg120+1)
-- Dumped by pg_dump version 17.4

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
-- Name: public; Type: SCHEMA; Schema: -; Owner: -
--

-- *not* creating schema, since initdb creates it


--
-- Name: SCHEMA public; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON SCHEMA public IS '';


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: acquisition; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.acquisition (
    acquisition_id integer NOT NULL,
    acquiring_object_id character varying(255),
    acquired_object_id character varying(255),
    term_code character varying(255),
    price_amount numeric(15,2),
    price_currency_code character varying(3),
    acquired_at timestamp without time zone,
    source_url text,
    source_description text,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: company; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.company (
    office_id integer NOT NULL,
    object_id character varying(255),
    description text,
    region character varying(255),
    address1 text,
    address2 text,
    city character varying(255),
    zip_code character varying(200),
    state_code character varying(255),
    country_code character varying(255),
    latitude numeric(9,6),
    longitude numeric(9,6),
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: funding_rounds; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.funding_rounds (
    funding_round_id integer NOT NULL,
    object_id character varying(255),
    funded_at date,
    funding_round_type character varying(255),
    funding_round_code character varying(255),
    raised_amount_usd numeric(15,2),
    raised_amount numeric(15,2),
    raised_currency_code character varying(255),
    pre_money_valuation_usd numeric(15,2),
    pre_money_valuation numeric(15,2),
    pre_money_currency_code character varying(255),
    post_money_valuation_usd numeric(15,2),
    post_money_valuation numeric(15,2),
    post_money_currency_code character varying(255),
    participants text,
    is_first_round boolean,
    is_last_round boolean,
    source_url text,
    source_description text,
    created_by character varying(255),
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: funds; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.funds (
    fund_id character varying(255) NOT NULL,
    object_id character varying(255),
    name character varying(255),
    funded_at date,
    raised_amount numeric(15,2),
    raised_currency_code character varying(3),
    source_url text,
    source_description text,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: investments; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.investments (
    investment_id integer NOT NULL,
    funding_round_id integer,
    funded_object_id character varying,
    investor_object_id character varying,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: ipos; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ipos (
    ipo_id character varying(255) NOT NULL,
    object_id character varying(255),
    valuation_amount numeric(15,2),
    valuation_currency_code character varying(3),
    raised_amount numeric(15,2),
    raised_currency_code character varying(3),
    public_at timestamp without time zone,
    stock_symbol character varying(255),
    source_url text,
    source_description text,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: acquisition acquisition_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.acquisition
    ADD CONSTRAINT acquisition_pkey PRIMARY KEY (acquisition_id);


--
-- Name: funding_rounds funding_rounds_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.funding_rounds
    ADD CONSTRAINT funding_rounds_pkey PRIMARY KEY (funding_round_id);


--
-- Name: funds funds_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.funds
    ADD CONSTRAINT funds_pkey PRIMARY KEY (fund_id);


--
-- Name: investments investments_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.investments
    ADD CONSTRAINT investments_pkey PRIMARY KEY (investment_id);


--
-- Name: ipos ipos_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ipos
    ADD CONSTRAINT ipos_pkey PRIMARY KEY (ipo_id);


--
-- Name: company offices_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company
    ADD CONSTRAINT offices_pkey PRIMARY KEY (office_id);


--
-- Name: company offices_un; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company
    ADD CONSTRAINT offices_un UNIQUE (object_id);


--
-- Name: acquisition acquisition_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.acquisition
    ADD CONSTRAINT acquisition_fk FOREIGN KEY (acquiring_object_id) REFERENCES public.company(object_id);


--
-- Name: acquisition acquisition_fk_1; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.acquisition
    ADD CONSTRAINT acquisition_fk_1 FOREIGN KEY (acquired_object_id) REFERENCES public.company(object_id);


--
-- Name: funding_rounds funding_rounds_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.funding_rounds
    ADD CONSTRAINT funding_rounds_fk FOREIGN KEY (object_id) REFERENCES public.company(object_id);


--
-- Name: funds funds_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.funds
    ADD CONSTRAINT funds_fk FOREIGN KEY (object_id) REFERENCES public.company(object_id);


--
-- Name: investments investments_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.investments
    ADD CONSTRAINT investments_fk FOREIGN KEY (funded_object_id) REFERENCES public.company(object_id);


--
-- Name: investments investments_fk_2; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.investments
    ADD CONSTRAINT investments_fk_2 FOREIGN KEY (investor_object_id) REFERENCES public.company(object_id);


--
-- Name: investments investments_fk_3; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.investments
    ADD CONSTRAINT investments_fk_3 FOREIGN KEY (funding_round_id) REFERENCES public.funding_rounds(funding_round_id);


--
-- Name: ipos ipos_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ipos
    ADD CONSTRAINT ipos_fk FOREIGN KEY (object_id) REFERENCES public.company(object_id);


--
-- PostgreSQL database dump complete
--

