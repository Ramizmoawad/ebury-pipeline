CREATE DATABASE airflow;
CREATE USER airflow WITH PASSWORD 'airflow';
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;

CREATE DATABASE ebury_dw;
CREATE USER dw_user WITH PASSWORD 'dw_password';
GRANT ALL PRIVILEGES ON DATABASE ebury_dw TO dw_user;

\c ebury_dw;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS audit;

GRANT ALL PRIVILEGES ON SCHEMA raw     TO dw_user;
GRANT ALL PRIVILEGES ON SCHEMA staging TO dw_user;
GRANT ALL PRIVILEGES ON SCHEMA marts   TO dw_user;
GRANT ALL PRIVILEGES ON SCHEMA audit   TO dw_user;

CREATE TABLE IF NOT EXISTS raw.customer_transactions (
    _row_id          SERIAL PRIMARY KEY,
    _loaded_at       TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    _source_file     TEXT,
    transaction_id   TEXT,
    customer_id      TEXT,
    transaction_date TEXT,
    product_id       TEXT,
    product_name     TEXT,
    quantity         TEXT,
    price            TEXT,
    tax              TEXT
);

CREATE TABLE IF NOT EXISTS audit.data_quality_log (
    id               SERIAL PRIMARY KEY,
    logged_at        TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    source_table     TEXT NOT NULL,
    row_identifier   TEXT,
    field_name       TEXT,
    issue_type       TEXT,
    raw_value        TEXT,
    resolution       TEXT,
    pipeline_run_id  TEXT
);

GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA raw   TO dw_user;
GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA audit TO dw_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA raw   TO dw_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA audit TO dw_user;
