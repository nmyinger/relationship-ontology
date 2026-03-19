-- db/setup_dev_db.sql
--
-- One-time setup script. Run as a Postgres superuser (e.g. postgres) against
-- the dealflow database BEFORE running 'make migrate' or 'make test'.
--
-- What it does:
--   1. Grants the dealflow user CREATE on the public schema so migrations can
--      create tables there. (Postgres 16 removed the historic default that
--      granted CREATE on public to all users.)
--   2. Grants the dealflow user CREATE on the database so test fixtures can
--      create isolated per-test schemas (CREATE SCHEMA test_slice2_xxxx).
--
-- How to run (one-time, as a superuser):
--
--   psql -h 127.0.0.1 -U postgres -d dealflow -f db/setup_dev_db.sql
--
-- Or interactively:
--
--   psql -h 127.0.0.1 -U postgres -d dealflow
--   \i db/setup_dev_db.sql
--
-- This script is safe to run more than once (GRANT is idempotent).

-- Allow the dealflow user to create tables in the public schema.
GRANT CREATE ON SCHEMA public TO dealflow;

-- Allow the dealflow user to create new schemas (needed by the test suite,
-- which creates an isolated schema per test run).
GRANT CREATE ON DATABASE dealflow TO dealflow;
