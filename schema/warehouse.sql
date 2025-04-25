-- =========================
-- DIMENSION TABLES
-- =========================

-- Extension for UUID
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";


CREATE TABLE dim_date (
    date_id int4 NOT NULL,
    date_actual date NOT NULL,
    day_suffix varchar(4) NOT NULL,
    day_name varchar(9) NOT NULL,
    day_of_year int4 NOT NULL,
    week_of_month int4 NOT NULL,
    week_of_year int4 NOT NULL,
    week_of_year_iso bpchar(10) NOT NULL,
    month_actual int4 NOT NULL,
    month_name varchar(9) NOT NULL,
    month_name_abbreviated bpchar(3) NOT NULL,
    quarter_actual int4 NOT NULL,
    quarter_name varchar(9) NOT NULL,
    year_actual int4 NOT NULL,
    first_day_of_week date NOT NULL,
    last_day_of_week date NOT NULL,
    first_day_of_month date NOT NULL,
    last_day_of_month date NOT NULL,
    first_day_of_quarter date NOT NULL,
    last_day_of_quarter date NOT NULL,
    first_day_of_year date NOT NULL,
    last_day_of_year date NOT NULL,
    mmyyyy bpchar(6) NOT NULL,
    mmddyyyy bpchar(10) NOT NULL,
    weekend_indr varchar(20) NOT NULL,
    CONSTRAINT dim_date_pkey PRIMARY KEY (date_id)
);


CREATE TABLE dim_company (
    company_id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    company_nk VARCHAR(255) UNIQUE,
    description TEXT,
    city VARCHAR(255),
    state_code VARCHAR(255),
    country_code VARCHAR(255),
    latitude DECIMAL(10,6),
    longitude DECIMAL(10,6),
    created_at timestamptz NULL DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE dim_people (
    people_id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    people_nk INTEGER UNIQUE,
    full_name VARCHAR(255),
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    affiliation VARCHAR(255),
    birthplace VARCHAR(255),
    created_at timestamptz NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_relationship (
    relationship_id INTEGER PRIMARY KEY,
    people_id INTEGER,
    company_id VARCHAR(255),
    title TEXT,
    start_at INTEGER,
    end_at INTEGER,
    relationship_status VARCHAR(255),
    relationship_order INT,
    created_at timestamptz NULL DEFAULT CURRENT_TIMESTAMP
    updated_at INTEGER,
    FOREIGN KEY (person_id) REFERENCES dim_people(person_id),
    FOREIGN KEY (company_id) REFERENCES dim_company(company_id),
    FOREIGN KEY (start_at) REFERENCES dim_datetime(datetime_key),
    FOREIGN KEY (end_at) REFERENCES dim_datetime(datetime_key),

);

-- =========================
-- FACT TABLES
-- =========================

CREATE TABLE fact_funding_rounds (
    funding_round_id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    funding_round_nk INTEGER UNIQUE,
    funded_at INTEGER,
    company_id UUID,  -- ✅ tipe disamakan dengan dim_company
    funding_round_type VARCHAR(255),
    funding_round_code VARCHAR(255),
    raised_amount_usd NUMERIC(15,2),
    pre_money_valuation_usd NUMERIC(15,2),
    post_money_valuation_usd NUMERIC(15,2),
    round_position_desc VARCHAR(50),
    round_stage_desc VARCHAR(50),
    created_at timestamptz NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (funded_at) REFERENCES dim_date(date_id),
    FOREIGN KEY (company_id) REFERENCES dim_company(company_id)
);

CREATE TABLE fact_acquisitions (
    acquisition_id INT PRIMARY KEY,
    acquiring_company_id VARCHAR(255),
    acquired_company_id VARCHAR(255),
    acquired_at INTEGER,
    price_amount NUMERIC,
    price_currency_code VARCHAR(255),
    term_code VARCHAR(255),
    created_at INTEGER,
    updated_at INTEGER,
    FOREIGN KEY (acquiring_company_id) REFERENCES dim_company(company_id),
    FOREIGN KEY (acquired_company_id) REFERENCES dim_company(company_id),
    FOREIGN KEY (acquired_at) REFERENCES dim_datetime(datetime_key),
    FOREIGN KEY (created_at) REFERENCES dim_datetime(datetime_key),
    FOREIGN KEY (updated_at) REFERENCES dim_datetime(datetime_key)
);

CREATE TABLE fact_ipos (
    ipo_id VARCHAR(255) PRIMARY KEY,
    company_id VARCHAR(255),
    public_at INTEGER,
    valuation_amount NUMERIC,
    raised_amount NUMERIC,
    valuation_currency_code VARCHAR(255),
    raised_currency_code VARCHAR(255),
    stock_symbol VARCHAR(255),
    created_at INTEGER,
    updated_at INTEGER,
    FOREIGN KEY (company_id) REFERENCES dim_company(company_id),
    FOREIGN KEY (public_at) REFERENCES dim_datetime(datetime_key),
    FOREIGN KEY (created_at) REFERENCES dim_datetime(datetime_key),
    FOREIGN KEY (updated_at) REFERENCES dim_datetime(datetime_key)
);

CREATE TABLE fact_funds (
    fund_id VARCHAR(255) PRIMARY KEY,
    company_id VARCHAR(255),
    funded_at INTEGER,
    fund_name VARCHAR(255),
    raised_amount NUMERIC,
    currency_code VARCHAR(255),
    created_at INTEGER,
    updated_at INTEGER,
    FOREIGN KEY (company_id) REFERENCES dim_company(company_id),
    FOREIGN KEY (funded_at) REFERENCES dim_datetime(datetime_key),
    FOREIGN KEY (created_at) REFERENCES dim_datetime(datetime_key),
    FOREIGN KEY (updated_at) REFERENCES dim_datetime(datetime_key)
);

CREATE TABLE fact_investments (
    investment_id INT PRIMARY KEY,
    funding_round_id INT,
    investor_company_id VARCHAR(255),
    investee_company_id VARCHAR(255),
    created_at INTEGER,
    updated_at INTEGER,
    FOREIGN KEY (funding_round_id) REFERENCES fact_funding_rounds(funding_round_id),
    FOREIGN KEY (investor_company_id) REFERENCES dim_company(company_id),
    FOREIGN KEY (investee_company_id) REFERENCES dim_company(company_id),
    FOREIGN KEY (created_at) REFERENCES dim_datetime(datetime_key),
    FOREIGN KEY (updated_at) REFERENCES dim_datetime(datetime_key)
);

CREATE TABLE fact_milestones (
    milestone_id VARCHAR(255) PRIMARY KEY,
    company_id VARCHAR(255),
    milestone_at INTEGER,
    description TEXT,
    milestone_code VARCHAR(255),
    created_at INTEGER,
    updated_at INTEGER,
    FOREIGN KEY (company_id) REFERENCES dim_company(company_id),
    FOREIGN KEY (milestone_at) REFERENCES dim_datetime(datetime_key),
    FOREIGN KEY (created_at) REFERENCES dim_datetime(datetime_key),
    FOREIGN KEY (updated_at) REFERENCES dim_datetime(datetime_key)
);


INSERT INTO dim_date
SELECT
    EXTRACT(YEAR FROM d)::INT * 10000 + EXTRACT(MONTH FROM d)::INT * 100 + EXTRACT(DAY FROM d)::INT AS date_id,
    d AS date_actual,
    TO_CHAR(d, 'FMDDth') AS day_suffix,
    TO_CHAR(d, 'Day') AS day_name,
    EXTRACT(DOY FROM d)::INT AS day_of_year,
    CEIL(EXTRACT(DAY FROM d) / 7.0)::INT AS week_of_month,
    EXTRACT(WEEK FROM d)::INT AS week_of_year,
    TO_CHAR(d, 'IYYY-IW') AS week_of_year_iso,
    EXTRACT(MONTH FROM d)::INT AS month_actual,
    TO_CHAR(d, 'Month') AS month_name,
    TO_CHAR(d, 'Mon') AS month_name_abbreviated,
    EXTRACT(QUARTER FROM d)::INT AS quarter_actual,
    CASE EXTRACT(QUARTER FROM d)
        WHEN 1 THEN 'First'
        WHEN 2 THEN 'Second'
        WHEN 3 THEN 'Third'
        WHEN 4 THEN 'Fourth'
    END AS quarter_name,
    EXTRACT(YEAR FROM d)::INT AS year_actual,
    date_trunc('week', d)::DATE AS first_day_of_week,
    (date_trunc('week', d)::DATE + INTERVAL '6 day')::DATE AS last_day_of_week,
    date_trunc('month', d)::DATE AS first_day_of_month,
    (date_trunc('month', d) + INTERVAL '1 month - 1 day')::DATE AS last_day_of_month,
    date_trunc('quarter', d)::DATE AS first_day_of_quarter,
    (date_trunc('quarter', d) + INTERVAL '3 month - 1 day')::DATE AS last_day_of_quarter,
    date_trunc('year', d)::DATE AS first_day_of_year,
    (date_trunc('year', d) + INTERVAL '1 year - 1 day')::DATE AS last_day_of_year,
    TO_CHAR(d, 'MMYYYY')::bpchar AS mmyyyy,
    TO_CHAR(d, 'MM/DD/YYYY')::bpchar AS mmddyyyy,
    CASE WHEN EXTRACT(ISODOW FROM d) IN (6, 7) THEN 'Weekend' ELSE 'Weekday' END AS weekend_indr
FROM generate_series(
    DATE '1900-01-01',
    DATE '2100-12-31',
    INTERVAL '1 day'
) AS g(d);
