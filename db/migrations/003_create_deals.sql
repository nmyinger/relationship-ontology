CREATE TABLE IF NOT EXISTS deals (
    deal_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    market TEXT,
    asset_type TEXT,
    size NUMERIC,
    stage TEXT,
    strategy_tags TEXT[],
    status TEXT NOT NULL DEFAULT 'active',
    owner_user_id TEXT
);
