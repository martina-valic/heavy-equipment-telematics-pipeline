-- The v4 connector delivers exactly once, so a Kafka (topic, partition, offset) lands at most once.
-- Duplicate event_ids from producer retries are expected (Silver dedupes them); duplicate offsets
-- would mean the sink replayed data. Returns every offset that landed more than once.
WITH landed AS (
    SELECT RECORD_METADATA FROM TELEMATICS.BRONZE.EQUIPMENT_TELEMETRY_RAW
    UNION ALL
    SELECT RECORD_METADATA FROM TELEMATICS.BRONZE.EQUIPMENT_TELEMETRY_DLQ_RAW
)
SELECT RECORD_METADATA:topic::STRING     AS topic,
       RECORD_METADATA:partition::NUMBER AS kafka_partition,
       RECORD_METADATA:offset::NUMBER    AS kafka_offset,
       COUNT(*)                          AS copies
FROM landed
GROUP BY 1, 2, 3
HAVING COUNT(*) > 1;
