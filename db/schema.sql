-- Tucker's Farm — Database Schema
-- Run this against your Supabase PostgreSQL to create the tables.
-- =============================================================================

-- Enable pgvector extension (for future Level 2 entity resolution)
CREATE EXTENSION IF NOT EXISTS vector;

-- =============================================================================
-- Table: business_entities  (The "Golden Record")
-- =============================================================================
-- Each row represents a unique real-world business, even if it appears on
-- multiple listing sites.  Populated by Level 2 deduplication (future phase).

CREATE TABLE IF NOT EXISTS business_entities (
    id              UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    derived_name    TEXT,
    primary_city    TEXT,
    primary_state   TEXT,
    primary_country TEXT DEFAULT 'US',
    aggregate_revenue   NUMERIC,
    primary_broker_email TEXT,
    confidence_score     REAL DEFAULT 0.0,
    -- pgvector embedding for semantic dedup (Level 2, future)
    description_embedding VECTOR(1536),
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- =============================================================================
-- Table: raw_listings  (The "Input" — every scraped row)
-- =============================================================================
-- Stores every row exactly as scraped.  The URL column is the Level 1 dedup
-- key: if a listing with the same URL already exists, we update instead of
-- insert.

CREATE TABLE IF NOT EXISTS raw_listings (
    id                  SERIAL PRIMARY KEY,
    url                 TEXT UNIQUE NOT NULL,
    listing_hash        TEXT NOT NULL,          -- SHA-256 of the URL
    source              TEXT NOT NULL,          -- 'BizBen', 'BizBuySell', etc.

    -- Core listing data (follows BizBen column set — the superset)
    title               TEXT DEFAULT 'N/A',
    city                TEXT DEFAULT 'N/A',
    state               TEXT DEFAULT 'N/A',
    country             TEXT DEFAULT 'US',
    industry            TEXT DEFAULT 'N/A',
    description         TEXT DEFAULT 'N/A',
    listed_by_firm      TEXT DEFAULT 'N/A',
    listed_by_name      TEXT DEFAULT 'N/A',
    phone               TEXT DEFAULT 'N/A',
    email               TEXT DEFAULT 'N/A',
    price               TEXT DEFAULT 'N/A',
    gross_revenue       TEXT DEFAULT 'N/A',
    cash_flow           TEXT DEFAULT 'N/A',
    inventory           TEXT DEFAULT 'N/A',
    ebitda              TEXT DEFAULT 'N/A',
    -- Normalized numeric fields used for range filters/sorting.
    price_num           NUMERIC,
    gross_revenue_num   NUMERIC,
    cash_flow_num       NUMERIC,
    ebitda_num          NUMERIC,
    financial_data      TEXT DEFAULT 'N/A',
    source_link         TEXT DEFAULT 'N/A',
    extra_information   TEXT DEFAULT 'N/A',
    deal_date           TEXT DEFAULT 'N/A',

    -- Scraping metadata
    first_seen_date     TIMESTAMPTZ DEFAULT NOW(),
    last_seen_date      TIMESTAMPTZ DEFAULT NOW(),
    scraping_date       TEXT,

    -- Level 2: link to the resolved business entity (nullable until resolved)
    business_entity_id  UUID REFERENCES business_entities(id) ON DELETE SET NULL
);

-- Normalize text-like financial values (e.g. "$1,200,000") to numeric.
CREATE OR REPLACE FUNCTION parse_financial_numeric(value TEXT)
RETURNS NUMERIC
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    cleaned TEXT;
BEGIN
    IF value IS NULL THEN
        RETURN NULL;
    END IF;

    cleaned := BTRIM(value);
    IF cleaned = '' OR UPPER(cleaned) IN ('N/A', 'NA', 'NULL', 'NONE', '-', '--') THEN
        RETURN NULL;
    END IF;

    -- Accounting format: "(123.45)" => "-123.45"
    IF cleaned ~ '^\(.*\)$' THEN
        cleaned := '-' || SUBSTRING(cleaned FROM 2 FOR CHAR_LENGTH(cleaned) - 2);
    END IF;

    cleaned := regexp_replace(cleaned, '[,$ ]', '', 'g');
    IF cleaned ~ '^[+-]?\d+(\.\d+)?$' THEN
        RETURN cleaned::NUMERIC;
    END IF;

    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION set_raw_listing_numeric_fields()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.price_num := parse_financial_numeric(NEW.price);
    NEW.gross_revenue_num := parse_financial_numeric(NEW.gross_revenue);
    NEW.cash_flow_num := parse_financial_numeric(NEW.cash_flow);
    NEW.ebitda_num := parse_financial_numeric(NEW.ebitda);
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_set_raw_listing_numeric_fields ON raw_listings;
CREATE TRIGGER trg_set_raw_listing_numeric_fields
BEFORE INSERT OR UPDATE OF price, gross_revenue, cash_flow, ebitda
ON raw_listings
FOR EACH ROW
EXECUTE FUNCTION set_raw_listing_numeric_fields();

-- =============================================================================
-- Indices
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_raw_listings_source
    ON raw_listings(source);

CREATE INDEX IF NOT EXISTS idx_raw_listings_industry
    ON raw_listings(industry);

CREATE INDEX IF NOT EXISTS idx_raw_listings_state
    ON raw_listings(state);

CREATE INDEX IF NOT EXISTS idx_raw_listings_country
    ON raw_listings(country);

CREATE INDEX IF NOT EXISTS idx_raw_listings_source_industry_state_country
    ON raw_listings(source, industry, state, country);

CREATE INDEX IF NOT EXISTS idx_raw_listings_email
    ON raw_listings(email);

CREATE INDEX IF NOT EXISTS idx_raw_listings_listing_hash
    ON raw_listings(listing_hash);

CREATE INDEX IF NOT EXISTS idx_raw_listings_business_entity_id
    ON raw_listings(business_entity_id);

CREATE INDEX IF NOT EXISTS idx_raw_listings_city_state
    ON raw_listings(city, state);

CREATE INDEX IF NOT EXISTS idx_raw_listings_price_num
    ON raw_listings(price_num)
    WHERE price_num IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_raw_listings_gross_revenue_num
    ON raw_listings(gross_revenue_num)
    WHERE gross_revenue_num IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_raw_listings_cash_flow_num
    ON raw_listings(cash_flow_num)
    WHERE cash_flow_num IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_raw_listings_ebitda_num
    ON raw_listings(ebitda_num)
    WHERE ebitda_num IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_raw_listings_last_seen_date
    ON raw_listings(last_seen_date DESC);

CREATE INDEX IF NOT EXISTS idx_raw_listings_first_seen_date
    ON raw_listings(first_seen_date DESC);

-- =============================================================================
-- Row Level Security (RLS) — Supabase requires this since RLS is enabled
-- =============================================================================
-- Allow the postgres role (used by your connection string) full access.

ALTER TABLE business_entities ENABLE ROW LEVEL SECURITY;
ALTER TABLE raw_listings ENABLE ROW LEVEL SECURITY;

-- Full access for the postgres role (service-level scripts)
CREATE POLICY "Allow full access for postgres" ON business_entities
    FOR ALL
    TO postgres
    USING (true)
    WITH CHECK (true);

CREATE POLICY "Allow full access for postgres" ON raw_listings
    FOR ALL
    TO postgres
    USING (true)
    WITH CHECK (true);

-- Read access for authenticated users (frontend / Supabase client)
CREATE POLICY "Allow read for authenticated" ON business_entities
    FOR SELECT
    TO authenticated
    USING (true);

CREATE POLICY "Allow read for authenticated" ON raw_listings
    FOR SELECT
    TO authenticated
    USING (true);
