-- Every Bronze row carries the Kafka lineage Silver relies on (topic, partition, offset, key,
-- produce time) and came from the topic mapped to its table. Returns offending rows.
WITH landed AS (
    SELECT 'EQUIPMENT_TELEMETRY_RAW' AS table_name, 'telematics.equipment.telemetry.v1' AS expected_topic, RECORD_METADATA
    FROM TELEMATICS.BRONZE.EQUIPMENT_TELEMETRY_RAW
    UNION ALL
    SELECT 'EQUIPMENT_TELEMETRY_DLQ_RAW', 'telematics.equipment.telemetry.dlq.v1', RECORD_METADATA
    FROM TELEMATICS.BRONZE.EQUIPMENT_TELEMETRY_DLQ_RAW
)
SELECT table_name, RECORD_METADATA
FROM landed
WHERE RECORD_METADATA:topic::STRING IS DISTINCT FROM expected_topic
   OR RECORD_METADATA:partition IS NULL
   OR RECORD_METADATA:offset IS NULL
   OR RECORD_METADATA:key IS NULL
   OR RECORD_METADATA:CreateTime IS NULL
   OR RECORD_METADATA:SnowflakeConnectorPushTime IS NULL
LIMIT 100;
