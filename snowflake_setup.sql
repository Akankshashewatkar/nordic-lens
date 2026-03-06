-- =============================================================================
-- NordicLens — Snowflake Setup
-- =============================================================================
-- Run this script once as ACCOUNTADMIN to bootstrap the full environment.
-- All statements are idempotent (IF NOT EXISTS / OR REPLACE where appropriate).
--
-- Execution order:
--   1. Warehouse
--   2. Database & schemas
--   3. Roles & grants
--   4. File format & external stage
--   5. RAW table DDLs
-- =============================================================================


-- =============================================================================
-- 1. WAREHOUSE
-- =============================================================================

USE ROLE SYSADMIN;

CREATE WAREHOUSE IF NOT EXISTS NORDICLENS_WH
    WAREHOUSE_SIZE   = 'X-SMALL'
    AUTO_SUSPEND     = 120          -- suspend after 2 min idle
    AUTO_RESUME      = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT          = 'NordicLens analytics warehouse';


-- =============================================================================
-- 2. DATABASE & SCHEMAS
-- =============================================================================

CREATE DATABASE IF NOT EXISTS NORDICLENS_DB
    COMMENT = 'NordicLens banking transaction analytics';

USE DATABASE NORDICLENS_DB;

-- Medallion architecture: RAW → STAGING → INTERMEDIATE → MARTS
CREATE SCHEMA IF NOT EXISTS RAW
    COMMENT = 'Immutable raw ingestion layer — COPY INTO only, no transforms';

CREATE SCHEMA IF NOT EXISTS STAGING
    COMMENT = 'dbt staging layer — type-cast, renamed, 1:1 with RAW sources';

CREATE SCHEMA IF NOT EXISTS INTERMEDIATE
    COMMENT = 'dbt intermediate layer — business logic, ephemeral models';

CREATE SCHEMA IF NOT EXISTS MARTS
    COMMENT = 'dbt marts layer — consumption-ready facts, dims, and reports';

CREATE SCHEMA IF NOT EXISTS SNAPSHOTS
    COMMENT = 'dbt SCD Type 2 snapshots — customer history';


-- =============================================================================
-- 3. ROLES & GRANTS
-- =============================================================================

USE ROLE SECURITYADMIN;

-- Application role: full read/write for the pipeline
CREATE ROLE IF NOT EXISTS NORDICLENS_ROLE
    COMMENT = 'NordicLens pipeline role — used by Airflow, dbt, and ingestion scripts';

-- Read-only role: used by the Streamlit dashboard and BI tools
CREATE ROLE IF NOT EXISTS NORDICLENS_REPORTER
    COMMENT = 'NordicLens read-only role — dashboard and BI consumers';

-- Role hierarchy: SYSADMIN owns both
GRANT ROLE NORDICLENS_ROLE     TO ROLE SYSADMIN;
GRANT ROLE NORDICLENS_REPORTER TO ROLE SYSADMIN;

-- Warehouse access
USE ROLE SYSADMIN;
GRANT USAGE ON WAREHOUSE NORDICLENS_WH TO ROLE NORDICLENS_ROLE;
GRANT USAGE ON WAREHOUSE NORDICLENS_WH TO ROLE NORDICLENS_REPORTER;

-- Database access
GRANT USAGE ON DATABASE NORDICLENS_DB TO ROLE NORDICLENS_ROLE;
GRANT USAGE ON DATABASE NORDICLENS_DB TO ROLE NORDICLENS_REPORTER;

-- Schema-level grants for NORDICLENS_ROLE (pipeline)
GRANT USAGE, CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT
    ON SCHEMA NORDICLENS_DB.RAW          TO ROLE NORDICLENS_ROLE;
GRANT USAGE, CREATE TABLE, CREATE VIEW
    ON SCHEMA NORDICLENS_DB.STAGING      TO ROLE NORDICLENS_ROLE;
GRANT USAGE, CREATE TABLE, CREATE VIEW
    ON SCHEMA NORDICLENS_DB.INTERMEDIATE TO ROLE NORDICLENS_ROLE;
GRANT USAGE, CREATE TABLE, CREATE VIEW
    ON SCHEMA NORDICLENS_DB.MARTS        TO ROLE NORDICLENS_ROLE;
GRANT USAGE, CREATE TABLE, CREATE VIEW
    ON SCHEMA NORDICLENS_DB.SNAPSHOTS    TO ROLE NORDICLENS_ROLE;

-- Table/view grants for NORDICLENS_ROLE
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
    ON ALL TABLES IN SCHEMA NORDICLENS_DB.RAW          TO ROLE NORDICLENS_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
    ON ALL TABLES IN SCHEMA NORDICLENS_DB.STAGING      TO ROLE NORDICLENS_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
    ON ALL TABLES IN SCHEMA NORDICLENS_DB.INTERMEDIATE TO ROLE NORDICLENS_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
    ON ALL TABLES IN SCHEMA NORDICLENS_DB.MARTS        TO ROLE NORDICLENS_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
    ON ALL TABLES IN SCHEMA NORDICLENS_DB.SNAPSHOTS    TO ROLE NORDICLENS_ROLE;

-- Future grants — auto-grant on new objects
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
    ON FUTURE TABLES IN SCHEMA NORDICLENS_DB.RAW          TO ROLE NORDICLENS_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
    ON FUTURE TABLES IN SCHEMA NORDICLENS_DB.STAGING      TO ROLE NORDICLENS_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
    ON FUTURE TABLES IN SCHEMA NORDICLENS_DB.INTERMEDIATE TO ROLE NORDICLENS_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
    ON FUTURE TABLES IN SCHEMA NORDICLENS_DB.MARTS        TO ROLE NORDICLENS_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
    ON FUTURE TABLES IN SCHEMA NORDICLENS_DB.SNAPSHOTS    TO ROLE NORDICLENS_ROLE;

-- Read-only grants for NORDICLENS_REPORTER (dashboard)
GRANT USAGE ON SCHEMA NORDICLENS_DB.MARTS     TO ROLE NORDICLENS_REPORTER;
GRANT USAGE ON SCHEMA NORDICLENS_DB.SNAPSHOTS TO ROLE NORDICLENS_REPORTER;
GRANT SELECT ON ALL TABLES IN SCHEMA NORDICLENS_DB.MARTS     TO ROLE NORDICLENS_REPORTER;
GRANT SELECT ON ALL TABLES IN SCHEMA NORDICLENS_DB.SNAPSHOTS TO ROLE NORDICLENS_REPORTER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA NORDICLENS_DB.MARTS     TO ROLE NORDICLENS_REPORTER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA NORDICLENS_DB.SNAPSHOTS TO ROLE NORDICLENS_REPORTER;


-- =============================================================================
-- 4. FILE FORMAT & EXTERNAL S3 STAGE
-- =============================================================================

USE ROLE NORDICLENS_ROLE;
USE SCHEMA NORDICLENS_DB.RAW;
USE WAREHOUSE NORDICLENS_WH;

CREATE FILE FORMAT IF NOT EXISTS NORDICLENS_DB.RAW.NORDICLENS_PARQUET
    TYPE                = 'PARQUET'
    SNAPPY_COMPRESSION  = TRUE
    BINARY_AS_TEXT      = FALSE
    COMMENT             = 'NordicLens Parquet file format — snappy compressed';

-- External stage pointing at the S3 raw data lake.
-- Replace <AWS_KEY_ID>, <AWS_SECRET_KEY>, and <S3_BUCKET> with your values,
-- or use a Snowflake storage integration (recommended for production).
CREATE STAGE IF NOT EXISTS NORDICLENS_DB.RAW.NORDICLENS_S3_STAGE
    URL                 = 's3://nordiclens-raw/'
    CREDENTIALS         = (
        AWS_KEY_ID      = '<AWS_KEY_ID>'      -- replace before running
        AWS_SECRET_KEY  = '<AWS_SECRET_KEY>'  -- replace before running
    )
    FILE_FORMAT         = NORDICLENS_DB.RAW.NORDICLENS_PARQUET
    COMMENT             = 'NordicLens raw S3 data lake stage';


-- =============================================================================
-- 5. RAW TABLE DDLs
-- =============================================================================

USE SCHEMA NORDICLENS_DB.RAW;

-- ---------------------------------------------------------------------------
-- RAW.TRANSACTIONS
-- Source: ingestion/extractors/transaction_simulator.py
-- Grain: one row per banking transaction
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS NORDICLENS_DB.RAW.TRANSACTIONS (
    transaction_id   VARCHAR(36)     NOT NULL    COMMENT 'UUID primary key',
    customer_id      VARCHAR(36)     NOT NULL    COMMENT 'Foreign key → RAW.CUSTOMERS',
    merchant_id      VARCHAR(36)     NOT NULL    COMMENT 'Deterministic UUID from merchant name',
    merchant_name    VARCHAR(255)                COMMENT 'Human-readable merchant name',
    mcc_code         VARCHAR(4)                  COMMENT '4-digit ISO 18245 Merchant Category Code',
    mcc_category     VARCHAR(100)                COMMENT 'Human-readable MCC category label',
    amount_nok       NUMBER(18, 2)   NOT NULL    COMMENT 'Transaction amount in Norwegian Krone',
    currency         VARCHAR(3)      DEFAULT 'NOK' COMMENT 'ISO 4217 currency code',
    transaction_type VARCHAR(20)                 COMMENT 'debit | credit | transfer',
    transaction_date TIMESTAMP_NTZ               COMMENT 'Transaction timestamp (UTC)',
    is_weekend       BOOLEAN                     COMMENT 'True if transaction occurred on Sat/Sun',
    hour_of_day      NUMBER(2, 0)                COMMENT 'Hour component of transaction_date (0–23)',
    is_flagged       BOOLEAN         DEFAULT FALSE COMMENT 'Simulator-injected fraud ground truth',
    country_code     VARCHAR(2)                  COMMENT 'ISO 3166-1 alpha-2 merchant country',
    channel          VARCHAR(20)                 COMMENT 'mobile | atm | online | branch',
    _loaded_at       TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP() COMMENT 'Row load timestamp'
)
COMMENT = 'Synthetic Norwegian banking transactions — 100k rows, 2023–2024';

-- ---------------------------------------------------------------------------
-- RAW.CUSTOMERS
-- Source: ingestion/extractors/transaction_simulator.py
-- Grain: one row per customer
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS NORDICLENS_DB.RAW.CUSTOMERS (
    customer_id      VARCHAR(36)     NOT NULL    COMMENT 'UUID primary key',
    first_name       VARCHAR(100)                COMMENT 'Norwegian first name (Faker no_NO)',
    last_name        VARCHAR(100)                COMMENT 'Norwegian last name',
    date_of_birth    DATE                        COMMENT 'Customer date of birth',
    gender           VARCHAR(1)                  COMMENT 'M | F',
    city             VARCHAR(100)                COMMENT 'Norwegian city of residence',
    postal_code      VARCHAR(10)                 COMMENT 'Norwegian postal code (4 digits)',
    is_domestic_only BOOLEAN         DEFAULT TRUE COMMENT 'True if customer never transacts abroad',
    customer_since   DATE                        COMMENT 'Date the customer relationship began',
    _loaded_at       TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP() COMMENT 'Row load timestamp'
)
COMMENT = 'Synthetic Norwegian customer master data — 5,000 customers';

-- ---------------------------------------------------------------------------
-- RAW.MACRO_CPI
-- Source: ingestion/extractors/ssb_api.py (Statistics Norway table 03013)
-- Grain: one row per month
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS NORDICLENS_DB.RAW.MACRO_CPI (
    period           DATE            NOT NULL    COMMENT 'First day of the observation month',
    cpi_index        NUMBER(10, 4)   NOT NULL    COMMENT 'CPI index value (base year 1998 = 100)',
    period_code      VARCHAR(10)                 COMMENT 'SSB time code, e.g. 2023M01',
    base_year        NUMBER(4, 0)    DEFAULT 1998 COMMENT 'CPI index base year',
    _source          VARCHAR(20)     DEFAULT 'SSB' COMMENT 'Data source identifier',
    _loaded_at       TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP() COMMENT 'Row load timestamp'
)
COMMENT = 'Norwegian Consumer Price Index from Statistics Norway (SSB), monthly';

-- ---------------------------------------------------------------------------
-- RAW.MACRO_RATES
-- Source: ingestion/extractors/norges_bank_api.py (Norges Bank SDMX API)
-- Grain: one row per rate-change business day
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS NORDICLENS_DB.RAW.MACRO_RATES (
    period           DATE            NOT NULL    COMMENT 'Date of the rate observation',
    value            NUMBER(10, 4)   NOT NULL    COMMENT 'Key policy rate in percent (e.g. 4.5)',
    rate_type        VARCHAR(30)     DEFAULT 'policy_rate_pct' COMMENT 'Rate type identifier',
    _source          VARCHAR(20)     DEFAULT 'NORGES_BANK' COMMENT 'Data source identifier',
    _loaded_at       TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP() COMMENT 'Row load timestamp'
)
COMMENT = 'Norges Bank key policy rate (styringsrenten), daily observations';


-- =============================================================================
-- 6. VERIFICATION QUERIES
-- =============================================================================
-- Run these after initial data load to confirm setup:
--
--   SHOW TABLES IN SCHEMA NORDICLENS_DB.RAW;
--   SHOW STAGES  IN SCHEMA NORDICLENS_DB.RAW;
--
--   SELECT COUNT(*) FROM NORDICLENS_DB.RAW.TRANSACTIONS;  -- expect ~100k
--   SELECT COUNT(*) FROM NORDICLENS_DB.RAW.CUSTOMERS;     -- expect  5,000
--   SELECT COUNT(*) FROM NORDICLENS_DB.RAW.MACRO_CPI;     -- expect   ~120 (2015–2025)
--   SELECT COUNT(*) FROM NORDICLENS_DB.RAW.MACRO_RATES;   -- expect    ~50 (rate changes)
--
--   SELECT MIN(transaction_date), MAX(transaction_date)
--   FROM   NORDICLENS_DB.RAW.TRANSACTIONS;
--
--   SELECT AVG(amount_nok), STDDEV(amount_nok)
--   FROM   NORDICLENS_DB.RAW.TRANSACTIONS;
-- =============================================================================
