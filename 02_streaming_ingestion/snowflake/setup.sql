-- Module 2: Snowflake objects for Kafka Connect ingestion into the Bronze tier.
--
-- Idempotent: safe to run any number of times. Run it in Snowsight as a user with ACCOUNTADMIN.
-- Do not run this template directly: generate_keypair.py renders it to
-- secrets/snowflake_setup.sql with the connector's RSA public key filled in.

-- ---------------------------------------------------------------------------
-- 1. Warehouse, database, Bronze schema, landing tables (owned by SYSADMIN)
-- ---------------------------------------------------------------------------
USE ROLE SYSADMIN;

-- Only used for queries (SLA checks, later dbt runs). Snowpipe Streaming ingestion is serverless.
CREATE WAREHOUSE IF NOT EXISTS TELEMATICS_WH
    WAREHOUSE_SIZE = XSMALL
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Heavy equipment telematics: query and transformation warehouse';

CREATE DATABASE IF NOT EXISTS TELEMATICS
    COMMENT = 'Heavy equipment telematics platform (Medallion: BRONZE, SILVER, GOLD)';

CREATE SCHEMA IF NOT EXISTS TELEMATICS.BRONZE
    COMMENT = 'Raw landing zone. Rows are written by Kafka Connect and never updated.';

-- Landing tables use the connector's non-schematized layout: the full Kafka record value in
-- RECORD_CONTENT and topic, partition, offset, key, CreateTime and SnowflakeConnectorPushTime
-- in RECORD_METADATA. Silver parses RECORD_CONTENT.
CREATE TABLE IF NOT EXISTS TELEMATICS.BRONZE.EQUIPMENT_TELEMETRY_RAW (
    RECORD_METADATA VARIANT COMMENT 'Kafka metadata added by the Snowflake connector',
    RECORD_CONTENT  VARIANT COMMENT 'Event payload (data contract equipment_telemetry.v1)'
)
COMMENT = 'Bronze landing for topic telematics.equipment.telemetry.v1';

CREATE TABLE IF NOT EXISTS TELEMATICS.BRONZE.EQUIPMENT_TELEMETRY_DLQ_RAW (
    RECORD_METADATA VARIANT COMMENT 'Kafka metadata added by the Snowflake connector',
    RECORD_CONTENT  VARIANT COMMENT 'DLQ envelope: failed_at, contract, errors, original_payload'
)
COMMENT = 'Bronze landing for topic telematics.equipment.telemetry.dlq.v1 (contract violations)';

-- Freshness and latency against the ingestion SLA (see the data contract).
--   seconds_since_last_event: how long since the newest event was produced to Kafka
--   p95_push_latency_seconds: Kafka produce time -> connector push to Snowflake, last hour
CREATE OR REPLACE VIEW TELEMATICS.BRONZE.INGESTION_FRESHNESS
    COMMENT = 'Bronze freshness and Kafka-to-Snowflake latency per landing table'
AS
WITH landed AS (
    SELECT 'EQUIPMENT_TELEMETRY_RAW' AS table_name,
           RECORD_METADATA:CreateTime::NUMBER                 AS kafka_create_ms,
           RECORD_METADATA:SnowflakeConnectorPushTime::NUMBER AS connector_push_ms
    FROM TELEMATICS.BRONZE.EQUIPMENT_TELEMETRY_RAW
    UNION ALL
    SELECT 'EQUIPMENT_TELEMETRY_DLQ_RAW',
           RECORD_METADATA:CreateTime::NUMBER,
           RECORD_METADATA:SnowflakeConnectorPushTime::NUMBER
    FROM TELEMATICS.BRONZE.EQUIPMENT_TELEMETRY_DLQ_RAW
)
SELECT
    table_name,
    COUNT(*)                                                  AS row_count,
    TO_TIMESTAMP_LTZ(MAX(kafka_create_ms), 3)                 AS last_event_at,
    DATEDIFF('second', last_event_at, CURRENT_TIMESTAMP())    AS seconds_since_last_event,
    APPROX_PERCENTILE(
        IFF(kafka_create_ms >= DATE_PART(EPOCH_MILLISECOND, DATEADD('hour', -1, CURRENT_TIMESTAMP())),
            (connector_push_ms - kafka_create_ms) / 1000, NULL),
        0.95)                                                 AS p95_push_latency_seconds
FROM landed
GROUP BY table_name;

-- ---------------------------------------------------------------------------
-- 2. Connector role and service user (SECURITYADMIN)
-- ---------------------------------------------------------------------------
USE ROLE SECURITYADMIN;

CREATE ROLE IF NOT EXISTS TELEMATICS_INGEST_ROLE
    COMMENT = 'Kafka Connect Snowflake sink: writes to TELEMATICS.BRONZE';
GRANT ROLE TELEMATICS_INGEST_ROLE TO ROLE SYSADMIN;

-- TYPE = SERVICE: no password and no interactive login. Key-pair authentication only.
CREATE USER IF NOT EXISTS TELEMATICS_KAFKA_CONNECTOR
    TYPE = SERVICE
    DEFAULT_ROLE = TELEMATICS_INGEST_ROLE
    DEFAULT_WAREHOUSE = TELEMATICS_WH
    COMMENT = 'Kafka Connect Snowflake sink connector (Module 2)';

-- Re-running sets the same key again. After a key rotation, re-render and re-run this file.
ALTER USER TELEMATICS_KAFKA_CONNECTOR SET RSA_PUBLIC_KEY = '<RSA_PUBLIC_KEY>';

GRANT ROLE TELEMATICS_INGEST_ROLE TO USER TELEMATICS_KAFKA_CONNECTOR;

-- The connector does not inherit privileges through the role hierarchy, so grant them directly.
-- Least privilege: tables are pre-created above (the connector runs with autocreate = none), so
-- it needs INSERT on them and CREATE PIPE for its default Snowpipe Streaming pipes.
GRANT USAGE       ON DATABASE TELEMATICS                              TO ROLE TELEMATICS_INGEST_ROLE;
GRANT USAGE       ON SCHEMA   TELEMATICS.BRONZE                       TO ROLE TELEMATICS_INGEST_ROLE;
GRANT CREATE PIPE ON SCHEMA   TELEMATICS.BRONZE                       TO ROLE TELEMATICS_INGEST_ROLE;
GRANT INSERT      ON TABLE    TELEMATICS.BRONZE.EQUIPMENT_TELEMETRY_RAW     TO ROLE TELEMATICS_INGEST_ROLE;
GRANT INSERT      ON TABLE    TELEMATICS.BRONZE.EQUIPMENT_TELEMETRY_DLQ_RAW TO ROLE TELEMATICS_INGEST_ROLE;

-- Read access and a warehouse, used by the SLA integration tests to confirm rows have landed.
GRANT USAGE  ON WAREHOUSE TELEMATICS_WH                                 TO ROLE TELEMATICS_INGEST_ROLE;
GRANT SELECT ON TABLE     TELEMATICS.BRONZE.EQUIPMENT_TELEMETRY_RAW     TO ROLE TELEMATICS_INGEST_ROLE;
GRANT SELECT ON TABLE     TELEMATICS.BRONZE.EQUIPMENT_TELEMETRY_DLQ_RAW TO ROLE TELEMATICS_INGEST_ROLE;
GRANT SELECT ON VIEW      TELEMATICS.BRONZE.INGESTION_FRESHNESS         TO ROLE TELEMATICS_INGEST_ROLE;

-- ---------------------------------------------------------------------------
-- 3. Verify
-- ---------------------------------------------------------------------------
SHOW GRANTS TO ROLE TELEMATICS_INGEST_ROLE;
DESC USER TELEMATICS_KAFKA_CONNECTOR;
