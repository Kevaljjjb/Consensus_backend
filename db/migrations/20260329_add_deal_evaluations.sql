-- Adds cached AI deal evaluations keyed by listing_id.

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

CREATE INDEX IF NOT EXISTS idx_deal_evaluations_listing_id
    ON deal_evaluations(listing_id);

CREATE INDEX IF NOT EXISTS idx_deal_evaluations_fit_score
    ON deal_evaluations(fit_score DESC);

ALTER TABLE deal_evaluations ENABLE ROW LEVEL SECURITY;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_policies
        WHERE schemaname = CURRENT_SCHEMA()
          AND tablename = 'deal_evaluations'
          AND policyname = 'Allow full access for postgres'
    ) THEN
        CREATE POLICY "Allow full access for postgres" ON deal_evaluations
            FOR ALL
            TO postgres
            USING (true)
            WITH CHECK (true);
    END IF;
END;
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_policies
        WHERE schemaname = CURRENT_SCHEMA()
          AND tablename = 'deal_evaluations'
          AND policyname = 'Allow read for authenticated'
    ) THEN
        CREATE POLICY "Allow read for authenticated" ON deal_evaluations
            FOR SELECT
            TO authenticated
            USING (true);
    END IF;
END;
$$;
