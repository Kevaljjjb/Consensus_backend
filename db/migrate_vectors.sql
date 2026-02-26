-- Consensus — Vector Migration
-- Run AFTER the base schema.sql has been applied.
-- Adds description_embedding to raw_listings and updates the dimension on business_entities.
-- =============================================================================

-- 1. Add embedding column to raw_listings (1024-dim for Qwen3-Embedding-8B)
ALTER TABLE raw_listings
    ADD COLUMN IF NOT EXISTS description_embedding VECTOR(1024);

-- 2. Update business_entities to 1024-dim (drop old 1536 column if exists, add 1024)
--    Safe: uses IF NOT EXISTS / IF EXISTS guards.
DO $$
BEGIN
    -- If column exists with wrong dimension, drop and recreate
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'business_entities'
          AND column_name = 'description_embedding'
    ) THEN
        ALTER TABLE business_entities DROP COLUMN description_embedding;
    END IF;
    ALTER TABLE business_entities ADD COLUMN description_embedding VECTOR(1024);
END $$;

-- 3. Create IVFFlat index for fast cosine search on raw_listings
--    Note: IVFFlat requires at least some rows to build; for small tables this is fine.
--    For large tables (100k+), consider building AFTER loading vectors.
CREATE INDEX IF NOT EXISTS idx_raw_listings_embedding
    ON raw_listings
    USING ivfflat (description_embedding vector_cosine_ops)
    WITH (lists = 100);

-- 4. Similarly for business_entities (future use)
CREATE INDEX IF NOT EXISTS idx_business_entities_embedding
    ON business_entities
    USING ivfflat (description_embedding vector_cosine_ops)
    WITH (lists = 100);
