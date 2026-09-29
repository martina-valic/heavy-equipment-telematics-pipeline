-- Rows in the main telemetry table passed the equipment_telemetry.v1 contract at the producer, so
-- every envelope field is present, readings is an array, and the Kafka key is the serial number.
-- Returns rows that break any of these (range and null-value anomalies are Silver's job, not this).
SELECT RECORD_METADATA:offset::NUMBER AS kafka_offset,
       RECORD_CONTENT
FROM TELEMATICS.BRONZE.EQUIPMENT_TELEMETRY_RAW
WHERE RECORD_CONTENT:schema_version::STRING NOT LIKE '1.%'
   OR RECORD_CONTENT:schema_version IS NULL
   OR RECORD_CONTENT:event_id IS NULL
   OR RECORD_CONTENT:event_timestamp IS NULL
   OR RECORD_CONTENT:sequence_number IS NULL
   OR RECORD_CONTENT:equipment_id IS NULL
   OR RECORD_CONTENT:equipment_serial_number IS NULL
   OR RECORD_CONTENT:type_name IS NULL
   OR RECORD_CONTENT:category IS NULL
   OR RECORD_CONTENT:operating_state IS NULL
   OR NOT IS_ARRAY(RECORD_CONTENT:readings)
   OR RECORD_METADATA:key::STRING IS DISTINCT FROM RECORD_CONTENT:equipment_serial_number::STRING
LIMIT 100;
