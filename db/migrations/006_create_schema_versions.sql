-- Documentation migration. The runner bootstraps this table before
-- reading migration files, so this is a no-op on first run.
-- It exists so schema_versions appears in the migration history.
CREATE TABLE IF NOT EXISTS schema_versions (
    migration_name TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
