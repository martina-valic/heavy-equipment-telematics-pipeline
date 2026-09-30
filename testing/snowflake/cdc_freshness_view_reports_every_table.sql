-- CDC_FRESHNESS (read by the Module 5 dashboards) returns one row per CDC landing table, the
-- initial snapshot has landed, and there are no negative ages or latencies.
-- Returns missing tables and impossible values.
WITH expected (table_name) AS (
    SELECT * FROM VALUES ('LEGACY_EQUIPMENT_CDC_RAW'), ('LEGACY_HOUR_METER_LOGS_CDC_RAW')
)
SELECT e.table_name, f.row_count, f.snapshot_rows, f.seconds_since_last_change,
       f.p95_commit_to_kafka_seconds, f.p95_commit_to_push_seconds,
       CASE WHEN f.table_name IS NULL THEN 'missing from view'
            WHEN f.snapshot_rows = 0 THEN 'initial snapshot has not landed'
            ELSE 'negative value' END AS problem
FROM expected e
LEFT JOIN TELEMATICS.BRONZE.CDC_FRESHNESS f ON f.table_name = e.table_name
WHERE f.table_name IS NULL
   OR f.snapshot_rows = 0
   OR f.seconds_since_last_change < 0
   OR f.p95_commit_to_kafka_seconds < 0
   OR f.p95_commit_to_push_seconds < 0;
