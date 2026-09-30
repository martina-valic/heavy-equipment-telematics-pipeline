-- One row per Bronze telemetry record, duplicate deliveries included, with the contract envelope typed.
-- The readings array stays as VARIANT: telemetry_readings flattens it.
with source as (
    select record_metadata, record_content
    from {{ source('bronze', 'equipment_telemetry_raw') }}
)

select
    record_content:event_id::string                                        as event_id,
    record_content:schema_version::string                                  as schema_version,
    record_content:event_timestamp::timestamp_tz                           as event_at,
    record_content:sequence_number::number                                 as sequence_number,
    record_content:equipment_id::number                                    as equipment_id,
    record_content:equipment_serial_number::string                         as equipment_serial_number,
    record_content:type_name::string                                       as type_name,
    record_content:category::string                                        as category,
    record_content:operating_state::string                                 as operating_state,
    record_content:readings                                                as readings,
    record_metadata:topic::string                                          as kafka_topic,
    record_metadata:partition::number                                      as kafka_partition,
    record_metadata:offset::number                                         as kafka_offset,
    to_timestamp_tz(record_metadata:CreateTime::number, 3)                 as produced_at,
    to_timestamp_tz(record_metadata:SnowflakeConnectorPushTime::number, 3) as ingested_at
from source
