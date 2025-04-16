-- =========================
-- DIMENSION TABLES
-- =========================

CREATE TABLE dim_datetime (
    datetime_key INTEGER PRIMARY KEY,      -- Format: YYYYMMDDHH24MISS
    full_datetime TIMESTAMP,               -- Contoh: 2009-05-24 10:42:44
    date_only DATE,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    day INTEGER,
    hour INTEGER,
    minute INTEGER,
    second INTEGER,
    weekday TEXT
);

CREATE TABLE dim_company (
    company_id uuid NOT NULL DEFAULT uuid_generate_v4(),
    company_nk VARCHAR(255) PRIMARY KEY,
    description TEXT,
    city VARCHAR(255),
    state_code VARCHAR(255),
    country_code VARCHAR(255),
    latitude DECIMAL(10,6),
    longitude DECIMAL(10,6),
    created_at INTEGER,
    updated_at INTEGER,
    created_at timestamptz NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_people (
    company_id uuid NOT NULL DEFAULT uuid_generate_v4(),
    people_nk INTEGER PRIMARY KEY,
    full_name VARCHAR(255),
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    affiliation VARCHAR(255),
    birthplace VARCHAR(255),
    created_at INTEGER,
    updated_at INTEGER,
    FOREIGN KEY (created_at) REFERENCES dim_datetime(datetime_key),
    FOREIGN KEY (updated_at) REFERENCES dim_datetime(datetime_key)
);

CREATE TABLE dim_relationship (
    relationship_id INTEGER PRIMARY KEY,
    person_id INTEGER,
    company_id VARCHAR(255),
    title TEXT,
    start_at INTEGER,
    end_at INTEGER,
    relationship_status VARCHAR(255),
    relationship_order INT,
    created_at INTEGER,
    updated_at INTEGER,
    FOREIGN KEY (person_id) REFERENCES dim_people(person_id),
    FOREIGN KEY (company_id) REFERENCES dim_company(company_id),
    FOREIGN KEY (start_at) REFERENCES dim_datetime(datetime_key),
    FOREIGN KEY (end_at) REFERENCES dim_datetime(datetime_key),
    FOREIGN KEY (created_at) REFERENCES dim_datetime(datetime_key),
    FOREIGN KEY (updated_at) REFERENCES dim_datetime(datetime_key)
);

-- =========================
-- FACT TABLES
-- =========================

CREATE TABLE fact_funding_rounds (
    funding_round_id INT PRIMARY KEY,
    funded_at INTEGER,
    company_id VARCHAR(255),
    funding_round_type VARCHAR(255),
    funding_round_code VARCHAR(255),
    raised_amount_usd NUMERIC(15,2),
    pre_money_valuation_usd NUMERIC(15,2),
    post_money_valuation_usd NUMERIC(15,2),
    round_position_desc VARCHAR(50),
    round_stage_desc VARCHAR(50),
    created_at INTEGER,
    updated_at INTEGER,
    FOREIGN KEY (funded_at) REFERENCES dim_datetime(datetime_key),
    FOREIGN KEY (company_id) REFERENCES dim_company(company_id),
    FOREIGN KEY (created_at) REFERENCES dim_datetime(datetime_key),
    FOREIGN KEY (updated_at) REFERENCES dim_datetime(datetime_key)
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
