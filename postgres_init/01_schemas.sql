-- Retail Analytics: base schema layout (raw -> staging -> warehouse)
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS warehouse;

-- Raw landing table for the UCI Online Retail II dataset (chunked-loaded by ingestion script)
CREATE TABLE IF NOT EXISTS raw.retail_transactions (
    id              BIGSERIAL PRIMARY KEY,
    invoice         TEXT,
    stock_code      TEXT,
    description     TEXT,
    quantity        INTEGER,
    invoice_date    TIMESTAMP,
    price           NUMERIC(12, 4),
    customer_id     TEXT,
    country         TEXT,
    source_sheet    TEXT,
    loaded_at       TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_retail_invoice ON raw.retail_transactions (invoice);
CREATE INDEX IF NOT EXISTS idx_raw_retail_customer ON raw.retail_transactions (customer_id);
CREATE INDEX IF NOT EXISTS idx_raw_retail_date ON raw.retail_transactions (invoice_date);
