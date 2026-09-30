-- Every Bronze landing table (telemetry, telemetry DLQ and the two CDC tables) exists with exactly
-- the connector's non-schematized layout: RECORD_METADATA and RECORD_CONTENT, both VARIANT.
-- Returns missing, extra or mistyped columns.
WITH expected (table_name, column_name, data_type) AS (
    SELECT * FROM VALUES
        ('EQUIPMENT_TELEMETRY_RAW',        'RECORD_METADATA', 'VARIANT'),
        ('EQUIPMENT_TELEMETRY_RAW',        'RECORD_CONTENT',  'VARIANT'),
        ('EQUIPMENT_TELEMETRY_DLQ_RAW',    'RECORD_METADATA', 'VARIANT'),
        ('EQUIPMENT_TELEMETRY_DLQ_RAW',    'RECORD_CONTENT',  'VARIANT'),
        ('LEGACY_EQUIPMENT_CDC_RAW',       'RECORD_METADATA', 'VARIANT'),
        ('LEGACY_EQUIPMENT_CDC_RAW',       'RECORD_CONTENT',  'VARIANT'),
        ('LEGACY_HOUR_METER_LOGS_CDC_RAW', 'RECORD_METADATA', 'VARIANT'),
        ('LEGACY_HOUR_METER_LOGS_CDC_RAW', 'RECORD_CONTENT',  'VARIANT')
),
actual AS (
    SELECT table_name, column_name, data_type
    FROM TELEMATICS.INFORMATION_SCHEMA.COLUMNS
    WHERE table_schema = 'BRONZE'
      AND table_name IN ('EQUIPMENT_TELEMETRY_RAW', 'EQUIPMENT_TELEMETRY_DLQ_RAW',
                         'LEGACY_EQUIPMENT_CDC_RAW', 'LEGACY_HOUR_METER_LOGS_CDC_RAW')
)
SELECT COALESCE(e.table_name, a.table_name)   AS table_name,
       COALESCE(e.column_name, a.column_name) AS column_name,
       e.data_type AS expected_type,
       a.data_type AS actual_type
FROM expected e
FULL OUTER JOIN actual a
  ON a.table_name = e.table_name AND a.column_name = e.column_name
WHERE e.data_type IS DISTINCT FROM a.data_type;
