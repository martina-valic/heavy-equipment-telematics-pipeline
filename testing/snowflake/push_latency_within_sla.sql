-- Ingestion SLA: over the last 15 minutes, the p95 time from Kafka produce to the connector's
-- push into Snowflake stays under $sla_seconds (INGESTION_SLA_SECONDS, default 120).
-- A short window keeps an old backlog (e.g. events produced while the connector was down) from
-- failing the test long after it drained. Passes trivially when nothing was produced recently.
WITH recent AS (
    SELECT 'EQUIPMENT_TELEMETRY_RAW' AS table_name, RECORD_METADATA FROM TELEMATICS.BRONZE.EQUIPMENT_TELEMETRY_RAW
    UNION ALL
    SELECT 'EQUIPMENT_TELEMETRY_DLQ_RAW', RECORD_METADATA FROM TELEMATICS.BRONZE.EQUIPMENT_TELEMETRY_DLQ_RAW
)
SELECT table_name,
       COUNT(*) AS rows_in_window,
       APPROX_PERCENTILE(
           (RECORD_METADATA:SnowflakeConnectorPushTime::NUMBER - RECORD_METADATA:CreateTime::NUMBER) / 1000,
           0.95) AS p95_push_latency_seconds
FROM recent
WHERE RECORD_METADATA:CreateTime::NUMBER
      >= DATE_PART(EPOCH_MILLISECOND, DATEADD('minute', -15, CURRENT_TIMESTAMP()))
GROUP BY table_name
HAVING p95_push_latency_seconds > $sla_seconds;
