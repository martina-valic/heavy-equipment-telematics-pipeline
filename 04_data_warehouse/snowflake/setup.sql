-- Module 4: Snowflake objects for the dbt project (Silver and Gold tiers).
--
-- Idempotent: safe to run any number of times. Run it in Snowsight as a user with ACCOUNTADMIN,
-- after the Module 2 and 3 setup files (they create the warehouse, database and Bronze tables).
-- Do not run this template directly: generate_dbt_keypair.py renders it to
-- secrets/snowflake_dbt_setup.sql with dbt's RSA public key filled in.

-- ---------------------------------------------------------------------------
-- 1. Silver and Gold schemas (owned by SYSADMIN)
-- ---------------------------------------------------------------------------
USE ROLE SYSADMIN;

CREATE SCHEMA IF NOT EXISTS TELEMATICS.SILVER
    COMMENT = 'Typed, deduplicated and quality-flagged data, built by dbt from BRONZE';
CREATE SCHEMA IF NOT EXISTS TELEMATICS.GOLD
    COMMENT = 'Star schema (dimensions, facts) and marts for reporting, built by dbt from SILVER';

-- ---------------------------------------------------------------------------
-- 2. Transform role and dbt service user (SECURITYADMIN)
-- ---------------------------------------------------------------------------
USE ROLE SECURITYADMIN;

CREATE ROLE IF NOT EXISTS TELEMATICS_TRANSFORM_ROLE
    COMMENT = 'dbt: reads TELEMATICS.BRONZE, builds TELEMATICS.SILVER and TELEMATICS.GOLD';
GRANT ROLE TELEMATICS_TRANSFORM_ROLE TO ROLE SYSADMIN;

-- TYPE = SERVICE: no password and no interactive login. Key-pair authentication only. A separate
-- user from the Kafka connector, so each has its own key and least-privilege role.
CREATE USER IF NOT EXISTS TELEMATICS_DBT
    TYPE = SERVICE
    DEFAULT_ROLE = TELEMATICS_TRANSFORM_ROLE
    DEFAULT_WAREHOUSE = TELEMATICS_WH
    COMMENT = 'dbt Silver/Gold transformations (Module 4)';

-- Re-running sets the same key again. After a key rotation, re-render and re-run this file.
ALTER USER TELEMATICS_DBT SET RSA_PUBLIC_KEY = '<RSA_PUBLIC_KEY>';
-- Epoch timestamps (Kafka and Debezium times) are converted in the session time zone: pin it.
ALTER USER TELEMATICS_DBT SET TIMEZONE = 'UTC';

GRANT ROLE TELEMATICS_TRANSFORM_ROLE TO USER TELEMATICS_DBT;

-- Compute for the dbt runs (the same XSMALL warehouse as the tests; it auto-suspends after 60 s).
GRANT USAGE ON WAREHOUSE TELEMATICS_WH TO ROLE TELEMATICS_TRANSFORM_ROLE;
GRANT USAGE ON DATABASE  TELEMATICS    TO ROLE TELEMATICS_TRANSFORM_ROLE;

-- Bronze: read only. FUTURE covers landing tables added by later modules.
GRANT USAGE  ON SCHEMA TELEMATICS.BRONZE                   TO ROLE TELEMATICS_TRANSFORM_ROLE;
GRANT SELECT ON ALL TABLES    IN SCHEMA TELEMATICS.BRONZE  TO ROLE TELEMATICS_TRANSFORM_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA TELEMATICS.BRONZE  TO ROLE TELEMATICS_TRANSFORM_ROLE;

-- Silver and Gold: dbt creates (and so owns) the tables and views it builds. No schema ownership,
-- so dbt cannot drop the schemas or grant access to them.
GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA TELEMATICS.SILVER TO ROLE TELEMATICS_TRANSFORM_ROLE;
GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA TELEMATICS.GOLD   TO ROLE TELEMATICS_TRANSFORM_ROLE;

-- ---------------------------------------------------------------------------
-- 3. Verify
-- ---------------------------------------------------------------------------
SHOW GRANTS TO ROLE TELEMATICS_TRANSFORM_ROLE;
SHOW FUTURE GRANTS IN SCHEMA TELEMATICS.BRONZE;
DESC USER TELEMATICS_DBT;
