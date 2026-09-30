#!/bin/bash
# Creates the two login roles of the legacy database and sets their passwords from the environment
# (POSTGRES_* in the repo-root .env, or the postgres-credentials Secret on Kubernetes).
#
#   POSTGRES_CDC_USER  Debezium: REPLICATION plus SELECT on the captured tables (for the snapshot)
#   POSTGRES_APP_USER  The legacy application (legacy_activity.py, tests): DML on the fleet tables
#
# Runs automatically on first start (docker-entrypoint-initdb.d). Idempotent: re-run it after
# changing a password in .env:
#   docker compose -f docker/docker-compose.yml exec postgres bash /docker-entrypoint-initdb.d/02_roles.sh
#
# docker-entrypoint.sh executes init scripts that have the executable bit and sources the rest
# (Kubernetes ConfigMap files, Git checkouts on Linux). So this script leaves shell options alone
# and fails explicitly; when sourced, a failure aborts the database initialisation.

: "${POSTGRES_CDC_USER:?POSTGRES_CDC_USER is not set}"
: "${POSTGRES_CDC_PASSWORD:?POSTGRES_CDC_PASSWORD is not set. Run 03_cdc_migration/postgres/configure_env.py}"
: "${POSTGRES_APP_USER:?POSTGRES_APP_USER is not set}"
: "${POSTGRES_APP_PASSWORD:?POSTGRES_APP_PASSWORD is not set. Run 03_cdc_migration/postgres/configure_env.py}"

# Passwords go in as psql variables and are quoted by format(), never spliced into the SQL text.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  -v db="$POSTGRES_DB" \
  -v cdc_user="$POSTGRES_CDC_USER" -v cdc_password="$POSTGRES_CDC_PASSWORD" \
  -v app_user="$POSTGRES_APP_USER" -v app_password="$POSTGRES_APP_PASSWORD" <<'SQL' \
  || { echo "02_roles.sh: creating roles failed" >&2; exit 1; }
SELECT format('CREATE ROLE %I', :'cdc_user')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'cdc_user') \gexec
SELECT format('CREATE ROLE %I', :'app_user')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'app_user') \gexec

SELECT format('ALTER ROLE %I WITH LOGIN REPLICATION PASSWORD %L', :'cdc_user', :'cdc_password') \gexec
SELECT format('ALTER ROLE %I WITH LOGIN NOREPLICATION PASSWORD %L', :'app_user', :'app_password') \gexec

-- Nobody but the owner creates objects in public.
REVOKE CREATE ON SCHEMA public FROM PUBLIC;

-- Debezium: read the captured tables for the initial snapshot. Streaming uses the replication
-- slot and the pre-created publication, which need only the REPLICATION attribute.
SELECT format('GRANT CONNECT ON DATABASE %I TO %I', :'db', :'cdc_user') \gexec
SELECT format('GRANT USAGE ON SCHEMA fleet TO %I', :'cdc_user') \gexec
SELECT format('GRANT SELECT ON fleet.equipment, fleet.hour_meter_logs TO %I', :'cdc_user') \gexec

-- Legacy application: day-to-day reads and writes, no DDL.
SELECT format('GRANT CONNECT ON DATABASE %I TO %I', :'db', :'app_user') \gexec
SELECT format('GRANT USAGE ON SCHEMA fleet TO %I', :'app_user') \gexec
SELECT format('GRANT SELECT, INSERT, UPDATE, DELETE ON fleet.equipment, fleet.hour_meter_logs TO %I', :'app_user') \gexec
SELECT format('GRANT USAGE ON ALL SEQUENCES IN SCHEMA fleet TO %I', :'app_user') \gexec
SQL

echo "roles ready: $POSTGRES_CDC_USER (CDC), $POSTGRES_APP_USER (application)"
