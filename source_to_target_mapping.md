# 🗂️ Source to Target Mapping – Startup Public Data Warehouse

---

## 📄 Source Table: `company` → Target Table: `dim_company`

| Source Column     | Source Type | Target Column   | Target Type     | Description                      |
|------------------|-------------|------------------|------------------|----------------------------------|
| object_id        | varchar (255)     | company_nk       | varchar          | Direct Mapping     |
| description      | text        | description      | text             | Direct Mapping                   |
| region           | varchar (255)     | region           | varchar (255)             | Direct Mapping                   |
| city             | varchar (255)     | city             | varchar (255)             | Direct Mapping                   |
| state_code       | varchar (255)     | state_code       | varchar (255)             | Direct Mapping                   |
| country_code     | varchar (255)     | country_code     | varchar (255)             | Direct Mapping                   |
| latitude         | decimal     | latitude         | decimal(10,6)    | Direct Mapping                   |
| longitude        | decimal     | longitude        | decimal(10,6)    | Direct Mapping                   |                  |
| office_id, address1, address2, zip_code,         |             | *not used*       |                  | Not relevant for analytics       |
---

## 📄 Source Table: `people` → Target Table: `dim_people`

| Source Column     | Source Type | Target Column   | Target Type     | Description                      |
|------------------|-------------|------------------|------------------|----------------------------------|
| people_id        | text     | people_nk        | int          | Direct Mapping                      |
| first_name       | text        | first_name       | varchar (255)              | Direct Mapping                   |
| last_name        | text        | last_name        | varchar (255)              | Direct Mapping                   |
| affiliation_name | text        | affiliation      | varchar (255)              | Renamed                          |
| birthplace       | text        | birthplace       | varchar (255)              | Direct Mapping                   |
|         | -           | full_name        | varchar (255)              | Derived: first_name + last_name  |
| *others*         |             | *not used*       |                  | Not relevant for analytics       |

---

## 📄 Source Table: `relationship` → Target Table: `dim_relationship`

| Source Column       | Source Type | Target Column       | Target Type     | Description                      |
|--------------------|-------------|----------------------|------------------|----------------------------------|
| relationship_id    | text     | relationship_nk      | int         | Direct Mapping                      |
| person_object_id   | text     | person_name            |  varchar (255)      | Lookup to dim_people, get person_name
| relationship_object_id | text | relationship_company_id           | uuid          | Lookup to dim_company       |
| title              | text        | title                | text             | Direct Mapping                   |
| start_at           | text        | start_at             | int             |  Formatted to YYYYMMDD and FK to dim_date.date_key |
| end_at             | text        | end_at               | int             |  Formatted to YYYYMMDD and FK to dim_date.date_key |
| is_past            | text     | relationship_status               | varchar (255)          | "Current" / "Past"                  |
| sequence            | text     | relationship_order              | int          | Direct Mapping                   |
| created_at           | text        | created_at             | timestamp             |  Formatted to YYYYMMDD and FK to dim_date.date_key |
| updated_at             | text        | updated_at               | timestamp             |  Formatted to YYYYMMDD and FK to dim_date.date_key |
| *others*         |             | *not used*       |                  | Not relevant for analytics       |
---

## 📄 Source Table: `funding_rounds` → Target Table: `fact_funding_rounds`

| Source Column           | Source Type | Target Column         | Target Type     | Description                                 |
|------------------------|-------------|------------------------|------------------|---------------------------------------------|
| funding_round_id       | int     | funding_round_nk       | int          | Direct Mapping                                 |
| object_id              | varchar (255)    | company_nk             | varchar (255)          | FK to dim_company                           |
| funded_at              | date        | funded_at              | int              | Formatted to YYYYMMDD and FK to dim_date.date_key |
| funding_round_type     | varchar (255)        | funding_round_type     | varchar (255)             | Direct Mapping                              |
| funding_round_code     | varchar (255)        | funding_round_code     | varchar (255)             | Direct Mapping                              |
| raised_amount_usd      | numeric(15,2)     | raised_amount_usd      | numeric(15,2)          | Direct Mapping                              |
| pre_money_valuation_usd| numeric(15,2)     | pre_money_valuation_usd| numeric(15,2)          | Direct Mapping                              |
| post_money_valuation_usd| numeric(15,2)    | post_money_valuation_usd| numeric(15,2)         | Direct Mapping                              |
| is_first_round         | boolean     | round_position_desc         | varchar(50)          | TRUE → 'First Round', FALSE → 'Not First Round'                              |
| is_last_round          | boolean     | round_stage_desc          | varchar(50)          | TRUE → 'Last Round', FALSE → 'Ongoing Round'                              |
| created_at           | timestamp        | created_at             | timestamp             |  Formatted to YYYYMMDD and FK to dim_date.date_key |
| updated_at             | timestamp        | updated_at               | timestamp             |  Formatted to YYYYMMDD and FK to dim_date.date_key |
---

## 📄 Source Table: `acquisition` → Target Table: `fact_acquisitions`

| Source Column        | Source Type | Target Column       | Target Type     | Description                            |
|---------------------|-------------|----------------------|------------------|----------------------------------------|
| acquisition_id      | int     | acquisition_nk       | int          | Direct Mapping                            |
| acquiring_object_id | varchar (255)     | acquiring_company_id | varchar (255)          | FK to dim_company                      |
| acquired_object_id  | varchar (255)     | acquired_company_id  | varchar (255)          | FK to dim_company                      |
| acquired_at         | date        | acquired_at             | int              | Formatted to YYYYMMDD and FK to dim_date.date_key |
| price_amount        | numeric(15,2)     | price_amount         | numeric(15,2)          | Direct Mapping
| price_currency_code        | varying(3)     | price_currency_code         | varying(255)          | Direct Mapping                             |
| term_code           | varchar (255)        | term_code            | varchar (255)             | Direct Mapping                         |
| created_at           | timestamp        | created_at             | timestamp             |  Formatted to YYYYMMDD and FK to dim_date.date_key |
| updated_at             | timestamp        | updated_at               | timestamp             |  Formatted to YYYYMMDD and FK to dim_date.date_key |
---

## 📄 Source Table: `ipos` → Target Table: `fact_ipos`

| Source Column       | Source Type | Target Column     | Target Type     | Description                              |
|--------------------|-------------|--------------------|------------------|------------------------------------------|
| ipo_id             | varchar (255)     | ipo_id             | varchar (255)          | Direct Mapping                              |
| object_id          | varchar (255)     | company_nk         | varchar (255)          | FK to dim_company                        |
| public_at          | date        | public_at           | int              | Formatted to YYYYMMDD and FK to dim_date.date_key |
| valuation_currency_code   | numeric     | valuation_currency_code   | numeric          | Direct Mapping                           |
| raised_currency_code      | varchar (255)      | raised_currency_code      | varying(255)           | Direct Mapping                           |
| valuation_amount      | numeric(15,2)     | valuation_amount      | numeric          | Direct Mapping                           |
| raised_amount      | numeric(15,2)     | raised_amount      | numeric(15,2)           | Direct Mapping                           |
| stock_symbol       | varchar (255)        | stock_symbol       | varchar (255)             | Direct Mapping                           |
| created_at           | timestamp        | created_at             | timestamp             |  Formatted to YYYYMMDD and FK to dim_date.date_key |
| updated_at             | timestamp        | updated_at               | timestamp             |  Formatted to YYYYMMDD and FK to dim_date.date_key |
---

## 📄 Source Table: `funds` → Target Table: `fact_funds`

| Source Column         | Source Type | Target Column   | Target Type     | Description                            |
|----------------------|-------------|------------------|------------------|----------------------------------------|
| fund_id              | varchar (255)     | fund_id          | varchar (255)          | Direct Mapping                            |
| object_id            | varchar (255)     | company_nk       | varchar (255)          | FK to dim_company                      |
| funded_at            | date        | funded_at         | int              | Formatted to YYYYMMDD and FK to dim_date.date_key |
| name                 | varchar (255)        | fund_name        | varchar (255)             | Renamed                                |
| raised_amount        | numeric(15,2)     | raised_amount    | numeric(15,2)          | Direct Mapping                         |
| raised_currency_code | varchar (255)        | raised_currency_code    | varchar (255)             | Direct Mapping                         |
| created_at           | timestamp        | created_at             | timestamp             |  Formatted to YYYYMMDD and FK to dim_date.date_key |
| updated_at             | timestamp        | updated_at               | timestamp             |  Formatted to YYYYMMDD and FK to dim_date.date_key |
---

## 📄 Source Table: `investment` → Target Table: `fact_investments`

| Source Column        | Source Type | Target Column       | Target Type     | Description                              |
|---------------------|-------------|----------------------|------------------|------------------------------------------|
| investment_id       | int     | investment_id        | int          | Direct Mapping                              |
| funding_round_id    | int     | funding_round_id     | int          | FK to fact_funding_rounds                |
| funded_object_id    | varchar     | investee_company_id  |  varchar (255)         | FK to dim_company                        |
| investor_object_id  | varchar     | investor_company_id  | varchar (255)          | FK to dim_company                        |
| created_at           | timestamp        | created_at             | timestamp             |  Formatted to YYYYMMDD and FK to dim_date.date_key |
| updated_at             | timestamp        | updated_at               | timestamp             |  Formatted to YYYYMMDD and FK to dim_date.date_key |
---

## 📄 Source Table: `milestones` (API) → Target Table: `fact_milestones`

| Source Column         | Source Type | Target Column     | Target Type     | Description                            |
|----------------------|-------------|--------------------|------------------|----------------------------------------|
| milestone_id         | varchar     | milestone_id       | varchar          | Direct Mapping                            |
| object_id            | varchar     | company_nk         | varchar          | FK to dim_company                      |
| milestone_at         | date        | milestone_at           | int              | Formatted to YYYYMMDD and FK to dim_date.date_key |
| description          | text        | description        | text             | Direct Mapping                         |
| milestone_code       | text        | milestone_code     | varcharb(255)             | Direct Mapping                         |
| created_at           | timestamp        | created_at             | timestamp             |  Formatted to YYYYMMDD and FK to dim_date.date_key |
| updated_at             | timestamp        | updated_at               | timestamp             |  Formatted to YYYYMMDD and FK to dim_date.date_key |
