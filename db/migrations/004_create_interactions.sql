-- Note: company_refs and deal_refs are UUID arrays, not foreign keys.
-- Postgres does not support FK constraints on array elements.
-- Referential integrity for these columns is enforced at the application layer.
CREATE TABLE IF NOT EXISTS interactions (
    interaction_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    direction TEXT,
    participants TEXT[],
    company_refs UUID[],
    deal_refs UUID[],
    summary TEXT,
    extracted_signals JSONB
);
