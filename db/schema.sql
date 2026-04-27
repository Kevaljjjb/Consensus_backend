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

-- =============================================================================
-- Table: deal_evaluations  (AI-powered deal scoring, cached per listing)
-- =============================================================================

CREATE TABLE IF NOT EXISTS deal_evaluations (
    id              SERIAL PRIMARY KEY,
    listing_id      INTEGER NOT NULL REFERENCES raw_listings(id) ON DELETE CASCADE,
    fit_score       INTEGER NOT NULL CHECK (fit_score >= 0 AND fit_score <= 100),
    score_breakdown JSONB NOT NULL DEFAULT '{}',
    pros            JSONB NOT NULL DEFAULT '[]',
    cons            JSONB NOT NULL DEFAULT '[]',
    summary         TEXT NOT NULL DEFAULT '',
    model_used      TEXT NOT NULL DEFAULT 'Qwen/Qwen3.5-27B',
    evaluated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(listing_id)
);

-- =============================================================================
-- Tables: chat_conversations / chat_messages  (User chat history)
-- =============================================================================

CREATE TABLE IF NOT EXISTS chat_conversations (
    id              UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    user_id         UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    title           TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS chat_messages (
    id              UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    conversation_id UUID NOT NULL REFERENCES chat_conversations(id) ON DELETE CASCADE,
    user_id         UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role            TEXT NOT NULL CHECK (role IN ('user', 'assistant', 'system')),
    content         TEXT NOT NULL DEFAULT '',
    message_order   INTEGER NOT NULL,
    edited_at       TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(conversation_id, message_order)
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

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at := NOW();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_set_chat_conversations_updated_at ON chat_conversations;
CREATE TRIGGER trg_set_chat_conversations_updated_at
BEFORE UPDATE ON chat_conversations
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

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

CREATE INDEX IF NOT EXISTS idx_deal_evaluations_listing_id
    ON deal_evaluations(listing_id);

CREATE INDEX IF NOT EXISTS idx_deal_evaluations_fit_score
    ON deal_evaluations(fit_score DESC);

CREATE INDEX IF NOT EXISTS idx_chat_conversations_user_id
    ON chat_conversations(user_id);

CREATE INDEX IF NOT EXISTS idx_chat_conversations_updated_at
    ON chat_conversations(updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_chat_messages_conversation_id
    ON chat_messages(conversation_id);

CREATE INDEX IF NOT EXISTS idx_chat_messages_user_id
    ON chat_messages(user_id);

CREATE INDEX IF NOT EXISTS idx_chat_messages_conversation_order
    ON chat_messages(conversation_id, message_order);

-- =============================================================================
-- Row Level Security (RLS) — Supabase requires this since RLS is enabled
-- =============================================================================
-- Allow the postgres role (used by your connection string) full access.

ALTER TABLE business_entities ENABLE ROW LEVEL SECURITY;
ALTER TABLE raw_listings ENABLE ROW LEVEL SECURITY;
ALTER TABLE deal_evaluations ENABLE ROW LEVEL SECURITY;
ALTER TABLE chat_conversations ENABLE ROW LEVEL SECURITY;
ALTER TABLE chat_messages ENABLE ROW LEVEL SECURITY;

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

CREATE POLICY "Allow full access for postgres" ON deal_evaluations
    FOR ALL
    TO postgres
    USING (true)
    WITH CHECK (true);

CREATE POLICY "Allow full access for postgres" ON chat_conversations
    FOR ALL
    TO postgres
    USING (true)
    WITH CHECK (true);

CREATE POLICY "Allow full access for postgres" ON chat_messages
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

CREATE POLICY "Allow read for authenticated" ON deal_evaluations
    FOR SELECT
    TO authenticated
    USING (true);

CREATE POLICY "Users can view their own conversations" ON chat_conversations
    FOR SELECT
    TO authenticated
    USING (auth.uid() = user_id);

CREATE POLICY "Users can insert their own conversations" ON chat_conversations
    FOR INSERT
    TO authenticated
    WITH CHECK (auth.uid() = user_id);

CREATE POLICY "Users can update their own conversations" ON chat_conversations
    FOR UPDATE
    TO authenticated
    USING (auth.uid() = user_id)
    WITH CHECK (auth.uid() = user_id);

CREATE POLICY "Users can delete their own conversations" ON chat_conversations
    FOR DELETE
    TO authenticated
    USING (auth.uid() = user_id);

CREATE POLICY "Users can view their own messages" ON chat_messages
    FOR SELECT
    TO authenticated
    USING (auth.uid() = user_id);

CREATE POLICY "Users can insert their own messages" ON chat_messages
    FOR INSERT
    TO authenticated
    WITH CHECK (auth.uid() = user_id);

CREATE POLICY "Users can update their own messages" ON chat_messages
    FOR UPDATE
    TO authenticated
    USING (auth.uid() = user_id)
    WITH CHECK (auth.uid() = user_id);

CREATE POLICY "Users can delete their own messages" ON chat_messages
    FOR DELETE
    TO authenticated
    USING (auth.uid() = user_id);
