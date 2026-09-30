-- Every CDC row holds a complete Debezium envelope (data contract legacy_fleet_cdc.v1) from the
-- expected source table: a known op, before/after present or null as that op requires, a source
-- LSN for streamed changes, and a Kafka key that matches the row (equipment_id for both tables).
-- Returns offending rows.
WITH landed AS (
    SELECT 'LEGACY_EQUIPMENT_CDC_RAW' AS table_name, 'equipment' AS expected_table, RECORD_METADATA, RECORD_CONTENT
    FROM TELEMATICS.BRONZE.LEGACY_EQUIPMENT_CDC_RAW
    UNION ALL
    SELECT 'LEGACY_HOUR_METER_LOGS_CDC_RAW', 'hour_meter_logs', RECORD_METADATA, RECORD_CONTENT
    FROM TELEMATICS.BRONZE.LEGACY_HOUR_METER_LOGS_CDC_RAW
),
events AS (
    SELECT table_name, expected_table, RECORD_METADATA, RECORD_CONTENT,
           RECORD_CONTENT:op::STRING AS op,
           -- A JSON null inside a VARIANT is not SQL NULL: IS_NULL_VALUE tells them apart.
           IS_NULL_VALUE(RECORD_CONTENT:before) AS before_is_null,
           IS_NULL_VALUE(RECORD_CONTENT:after)  AS after_is_null,
           COALESCE(RECORD_CONTENT:after:equipment_id, RECORD_CONTENT:before:equipment_id)::NUMBER AS row_equipment_id
    FROM landed
)
SELECT table_name, op, RECORD_METADATA, RECORD_CONTENT
FROM events
WHERE op IS NULL OR op NOT IN ('r', 'c', 'u', 'd')
   OR RECORD_CONTENT:source:db::STRING     IS DISTINCT FROM 'telematics_legacy'
   OR RECORD_CONTENT:source:schema::STRING IS DISTINCT FROM 'fleet'
   OR RECORD_CONTENT:source:table::STRING  IS DISTINCT FROM expected_table
   OR RECORD_CONTENT:source:ts_ms IS NULL
   OR RECORD_CONTENT:ts_ms IS NULL
   OR (op <> 'r' AND RECORD_CONTENT:source:lsn IS NULL)
   OR (op IN ('r', 'c') AND NOT (before_is_null AND NOT after_is_null))
   OR (op = 'u' AND (before_is_null OR after_is_null))
   OR (op = 'd' AND NOT (after_is_null AND NOT before_is_null))
   OR TRY_PARSE_JSON(RECORD_METADATA:key::STRING):equipment_id::NUMBER IS DISTINCT FROM row_equipment_id
LIMIT 100;
