-- INGESTION_FRESHNESS (read by the Module 5 dashboards) returns one row per Bronze landing table,
-- with no negative ages or latencies. Returns missing tables and impossible values.
WITH expected (table_name) AS (
    SELECT * FROM VALUES ('EQUIPMENT_TELEMETRY_RAW'), ('EQUIPMENT_TELEMETRY_DLQ_RAW')
)
SELECT e.table_name, f.row_count, f.seconds_since_last_event, f.p95_push_latency_seconds,
       IFF(f.table_name IS NULL, 'missing from view', 'negative value') AS problem
FROM expected e
LEFT JOIN TELEMATICS.BRONZE.INGESTION_FRESHNESS f ON f.table_name = e.table_name
WHERE f.table_name IS NULL
   OR f.row_count = 0
   OR f.seconds_since_last_event < 0
   OR f.p95_push_latency_seconds < 0;
