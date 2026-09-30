-- One row per event the producer rejected against the telemetry contract (the DLQ envelope).
-- The original payload failed validation, so its fields are read with TRY_ casts: any of them may
-- be missing or have the wrong type.
with source as (
    select record_metadata, record_content
    from {{ source('bronze', 'equipment_telemetry_dlq_raw') }}
)

select
    {{ dbt_utils.generate_surrogate_key([
        "record_metadata:topic::string",
        "record_metadata:partition::number",
        "record_metadata:offset::number",
    ]) }}                                                                    as violation_id,
    record_content:failed_at::timestamp_tz                                   as failed_at,
    record_content:contract::string                                          as contract,
    record_content:errors                                                    as errors,
    array_size(record_content:errors)                                        as error_count,
    record_content:errors[0]::string                                         as first_error,
    record_content:original_payload                                          as original_payload,
    record_content:original_payload:event_id::string                         as event_id,
    try_to_number(record_content:original_payload:equipment_id::string)      as equipment_id,
    record_content:original_payload:equipment_serial_number::string          as equipment_serial_number,
    try_to_timestamp_tz(record_content:original_payload:event_timestamp::string) as event_at,
    record_metadata:topic::string                                            as kafka_topic,
    record_metadata:partition::number                                        as kafka_partition,
    record_metadata:offset::number                                           as kafka_offset,
    to_timestamp_tz(record_metadata:CreateTime::number, 3)                   as produced_at,
    to_timestamp_tz(record_metadata:SnowflakeConnectorPushTime::number, 3)   as ingested_at
from source
