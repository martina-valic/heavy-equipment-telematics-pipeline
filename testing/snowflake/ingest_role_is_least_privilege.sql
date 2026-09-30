-- TELEMATICS_INGEST_ROLE holds only what the Module 2 and 3 setup.sql files grant: it can insert
-- into and read Bronze, but cannot update, delete, truncate, create tables or own anything
-- outside its streaming pipes.
-- Returns any grant outside that allowlist.
SHOW GRANTS TO ROLE TELEMATICS_INGEST_ROLE;

WITH allowed (privilege, granted_on, name) AS (
    SELECT * FROM VALUES
        ('USAGE',       'DATABASE',  'TELEMATICS'),
        ('USAGE',       'SCHEMA',    'TELEMATICS.BRONZE'),
        ('CREATE PIPE', 'SCHEMA',    'TELEMATICS.BRONZE'),
        ('INSERT',      'TABLE',     'TELEMATICS.BRONZE.EQUIPMENT_TELEMETRY_RAW'),
        ('SELECT',      'TABLE',     'TELEMATICS.BRONZE.EQUIPMENT_TELEMETRY_RAW'),
        ('INSERT',      'TABLE',     'TELEMATICS.BRONZE.EQUIPMENT_TELEMETRY_DLQ_RAW'),
        ('SELECT',      'TABLE',     'TELEMATICS.BRONZE.EQUIPMENT_TELEMETRY_DLQ_RAW'),
        ('SELECT',      'VIEW',      'TELEMATICS.BRONZE.INGESTION_FRESHNESS'),
        ('INSERT',      'TABLE',     'TELEMATICS.BRONZE.LEGACY_EQUIPMENT_CDC_RAW'),
        ('SELECT',      'TABLE',     'TELEMATICS.BRONZE.LEGACY_EQUIPMENT_CDC_RAW'),
        ('INSERT',      'TABLE',     'TELEMATICS.BRONZE.LEGACY_HOUR_METER_LOGS_CDC_RAW'),
        ('SELECT',      'TABLE',     'TELEMATICS.BRONZE.LEGACY_HOUR_METER_LOGS_CDC_RAW'),
        ('SELECT',      'VIEW',      'TELEMATICS.BRONZE.CDC_FRESHNESS'),
        ('USAGE',       'WAREHOUSE', 'TELEMATICS_WH')
),
granted AS (
    SELECT "privilege" AS privilege, "granted_on" AS granted_on, "name" AS name
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
)
SELECT g.*
FROM granted g
LEFT JOIN allowed a
  ON a.privilege = g.privilege AND a.granted_on = g.granted_on AND a.name = g.name
WHERE a.privilege IS NULL
  -- Pipes the connector creates for itself in Bronze are fine.
  AND NOT (g.granted_on = 'PIPE' AND g.name LIKE 'TELEMATICS.BRONZE.%');
