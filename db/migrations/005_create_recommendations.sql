CREATE TABLE IF NOT EXISTS recommendations (
    recommendation_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    date DATE NOT NULL,
    person_id UUID NOT NULL REFERENCES persons(person_id),
    related_deal_id UUID REFERENCES deals(deal_id),
    priority_score FLOAT NOT NULL,
    why_now TEXT NOT NULL,
    suggested_action TEXT NOT NULL,
    draft_text TEXT,
    status TEXT NOT NULL DEFAULT 'pending'
);
