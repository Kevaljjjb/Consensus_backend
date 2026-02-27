-- Adds normalized numeric columns and filter indexes for listing feed APIs.

ALTER TABLE raw_listings
    ADD COLUMN IF NOT EXISTS price_num NUMERIC,
    ADD COLUMN IF NOT EXISTS gross_revenue_num NUMERIC,
    ADD COLUMN IF NOT EXISTS cash_flow_num NUMERIC,
    ADD COLUMN IF NOT EXISTS ebitda_num NUMERIC;

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

-- Backfill existing rows.
UPDATE raw_listings
SET
    price_num = parse_financial_numeric(price),
    gross_revenue_num = parse_financial_numeric(gross_revenue),
    cash_flow_num = parse_financial_numeric(cash_flow),
    ebitda_num = parse_financial_numeric(ebitda);

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
