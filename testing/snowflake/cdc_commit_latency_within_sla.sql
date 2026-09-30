-- CDC ingestion SLA: over the last 15 minutes, the p95 time from the Postgres commit to the
-- connector's push into Snowflake stays under $sla_seconds (INGESTION_SLA_SECONDS, default 120).
-- Snapshot reads are excluded (their source time is the snapshot start). Passes trivially when
-- nothing changed recently, so run legacy_activity.py first for a meaningful result.
WITH recent AS (
    SELECT 'LEGACY_EQUIPMENT_CDC_RAW' AS table_name, RECORD_METADATA, RECORD_CONTENT
    FROM TELEMATICS.BRONZE.LEGACY_EQUIPMENT_CDC_RAW
    UNION ALL
    SELECT 'LEGACY_HOUR_METER_LOGS_CDC_RAW', RECORD_METADATA, RECORD_CONTENT
    FROM TELEMATICS.BRONZE.LEGACY_HOUR_METER_LOGS_CDC_RAW
)
SELECT table_name,
       COUNT(*) AS changes_in_window,
       APPROX_PERCENTILE(
           (RECORD_METADATA:SnowflakeConnectorPushTime::NUMBER - RECORD_CONTENT:source:ts_ms::NUMBER) / 1000,
           0.95) AS p95_commit_to_push_seconds
FROM recent
WHERE RECORD_CONTENT:op::STRING <> 'r'
  AND RECORD_CONTENT:source:ts_ms::NUMBER
      >= DATE_PART(EPOCH_MILLISECOND, DATEADD('minute', -15, CURRENT_TIMESTAMP()))
GROUP BY table_name
HAVING p95_commit_to_push_seconds > $sla_seconds;
