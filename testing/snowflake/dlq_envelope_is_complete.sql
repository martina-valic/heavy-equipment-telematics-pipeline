-- Every DLQ row is a complete envelope from the producer: when it failed, which contract, at
-- least one error, and the original payload. Returns incomplete envelopes.
SELECT RECORD_METADATA:offset::NUMBER AS kafka_offset,
       RECORD_CONTENT
FROM TELEMATICS.BRONZE.EQUIPMENT_TELEMETRY_DLQ_RAW
WHERE TRY_TO_TIMESTAMP_TZ(RECORD_CONTENT:failed_at::STRING) IS NULL
   OR RECORD_CONTENT:contract::STRING IS DISTINCT FROM 'equipment_telemetry.v1'
   OR NOT IS_ARRAY(RECORD_CONTENT:errors)
   OR ARRAY_SIZE(RECORD_CONTENT:errors) = 0
   OR NOT IS_OBJECT(RECORD_CONTENT:original_payload)
LIMIT 100;
