CREATE TABLE IF NOT EXISTS persons (
    person_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    full_name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    company_id UUID REFERENCES companies(company_id),
    title TEXT,
    last_contact_at TIMESTAMPTZ,
    relationship_strength FLOAT,
    responsiveness_score FLOAT,
    priority_override TEXT,
    tags TEXT[]
);
