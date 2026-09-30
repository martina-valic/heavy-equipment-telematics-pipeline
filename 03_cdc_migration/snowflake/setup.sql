-- Module 3: Snowflake Bronze landing tables for the Debezium CDC topics.
--
-- Idempotent: safe to run any number of times. Run it in Snowsight as ACCOUNTADMIN, after the
-- Module 2 setup (02_streaming_ingestion/snowflake/setup.sql), which creates the warehouse,
-- database, BRONZE schema, TELEMATICS_INGEST_ROLE and the connector's service user.
-- It contains no secrets, so it runs as is (no rendering step).

-- ---------------------------------------------------------------------------
-- 1. Landing tables and freshness view (SYSADMIN)
-- ---------------------------------------------------------------------------
USE ROLE SYSADMIN;

-- Same non-schematized layout as the telemetry tables. RECORD_CONTENT holds the full Debezium
-- envelope (before, after, source, op, ts_ms; data contract legacy_fleet_cdc.v1), and
-- RECORD_METADATA:key the row key as JSON. Rows are append-only: an UPDATE or DELETE in Postgres
-- is a new row here, and Silver (Module 4) rebuilds current state and history from the log.
CREATE TABLE IF NOT EXISTS TELEMATICS.BRONZE.LEGACY_EQUIPMENT_CDC_RAW (
    RECORD_METADATA VARIANT COMMENT 'Kafka metadata added by the Snowflake connector',
    RECORD_CONTENT  VARIANT COMMENT 'Debezium change event for fleet.equipment (data contract legacy_fleet_cdc.v1)'
)
COMMENT = 'Bronze landing for topic telematics.legacy.equipment.cdc.v1 (CDC of fleet.equipment)';

CREATE TABLE IF NOT EXISTS TELEMATICS.BRONZE.LEGACY_HOUR_METER_LOGS_CDC_RAW (
    RECORD_METADATA VARIANT COMMENT 'Kafka metadata added by the Snowflake connector',
    RECORD_CONTENT  VARIANT COMMENT 'Debezium change event for fleet.hour_meter_logs (data contract legacy_fleet_cdc.v1)'
)
COMMENT = 'Bronze landing for topic telematics.legacy.hour_meter_logs.cdc.v1 (CDC of fleet.hour_meter_logs)';

-- CDC freshness and end-to-end latency against the ingestion SLA. Latency is measured from the
-- Postgres commit (source.ts_ms), so it covers Debezium, Kafka and the sink. Snapshot reads
-- (op = 'r') are excluded from latency: their source time is the snapshot start, not a commit.
--   seconds_since_last_change:   how long since the newest committed change (quiet tables grow this)
--   p95_commit_to_kafka_seconds: Postgres commit -> Debezium produce, last hour
--   p95_commit_to_push_seconds:  Postgres commit -> connector push to Snowflake, last hour
CREATE OR REPLACE VIEW TELEMATICS.BRONZE.CDC_FRESHNESS
    COMMENT = 'Bronze CDC freshness and Postgres-commit-to-Snowflake latency per landing table'
AS
WITH landed AS (
    SELECT 'LEGACY_EQUIPMENT_CDC_RAW' AS table_name, RECORD_METADATA, RECORD_CONTENT
    FROM TELEMATICS.BRONZE.LEGACY_EQUIPMENT_CDC_RAW
    UNION ALL
    SELECT 'LEGACY_HOUR_METER_LOGS_CDC_RAW', RECORD_METADATA, RECORD_CONTENT
    FROM TELEMATICS.BRONZE.LEGACY_HOUR_METER_LOGS_CDC_RAW
),
changes AS (
    SELECT table_name,
           RECORD_CONTENT:op::STRING                          AS op,
           RECORD_CONTENT:source:ts_ms::NUMBER                AS commit_ms,
           RECORD_METADATA:CreateTime::NUMBER                 AS kafka_create_ms,
           RECORD_METADATA:SnowflakeConnectorPushTime::NUMBER AS connector_push_ms,
           op <> 'r' AND commit_ms >= DATE_PART(EPOCH_MILLISECOND, DATEADD('hour', -1, CURRENT_TIMESTAMP()))
                                                              AS in_latency_window
    FROM landed
)
SELECT
    table_name,
    COUNT(*)                                                           AS row_count,
    COUNT_IF(op = 'r')                                                 AS snapshot_rows,
    COUNT_IF(op = 'c')                                                 AS inserts,
    COUNT_IF(op = 'u')                                                 AS updates,
    COUNT_IF(op = 'd')                                                 AS deletes,
    TO_TIMESTAMP_LTZ(MAX(commit_ms), 3)                                AS last_change_at,
    DATEDIFF('second', last_change_at, CURRENT_TIMESTAMP())            AS seconds_since_last_change,
    APPROX_PERCENTILE(IFF(in_latency_window, (kafka_create_ms - commit_ms) / 1000, NULL), 0.95)
                                                                       AS p95_commit_to_kafka_seconds,
    APPROX_PERCENTILE(IFF(in_latency_window, (connector_push_ms - commit_ms) / 1000, NULL), 0.95)
                                                                       AS p95_commit_to_push_seconds
FROM changes
GROUP BY table_name;

-- ---------------------------------------------------------------------------
-- 2. Grants to the existing connector role (SECURITYADMIN)
-- ---------------------------------------------------------------------------
USE ROLE SECURITYADMIN;

-- Same least-privilege pattern as Module 2: INSERT for the sink, SELECT for the SLA tests.
GRANT INSERT ON TABLE TELEMATICS.BRONZE.LEGACY_EQUIPMENT_CDC_RAW       TO ROLE TELEMATICS_INGEST_ROLE;
GRANT INSERT ON TABLE TELEMATICS.BRONZE.LEGACY_HOUR_METER_LOGS_CDC_RAW TO ROLE TELEMATICS_INGEST_ROLE;
GRANT SELECT ON TABLE TELEMATICS.BRONZE.LEGACY_EQUIPMENT_CDC_RAW       TO ROLE TELEMATICS_INGEST_ROLE;
GRANT SELECT ON TABLE TELEMATICS.BRONZE.LEGACY_HOUR_METER_LOGS_CDC_RAW TO ROLE TELEMATICS_INGEST_ROLE;
GRANT SELECT ON VIEW  TELEMATICS.BRONZE.CDC_FRESHNESS                  TO ROLE TELEMATICS_INGEST_ROLE;

-- ---------------------------------------------------------------------------
-- 3. Verify
-- ---------------------------------------------------------------------------
SHOW GRANTS TO ROLE TELEMATICS_INGEST_ROLE;
