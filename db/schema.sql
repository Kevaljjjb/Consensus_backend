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

-- =============================================================================
-- Indices
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_raw_listings_source
    ON raw_listings(source);

CREATE INDEX IF NOT EXISTS idx_raw_listings_email
    ON raw_listings(email);

CREATE INDEX IF NOT EXISTS idx_raw_listings_listing_hash
    ON raw_listings(listing_hash);

CREATE INDEX IF NOT EXISTS idx_raw_listings_business_entity_id
    ON raw_listings(business_entity_id);

CREATE INDEX IF NOT EXISTS idx_raw_listings_city_state
    ON raw_listings(city, state);

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
