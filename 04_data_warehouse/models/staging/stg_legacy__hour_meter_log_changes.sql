-- One row per Debezium change event for fleet.hour_meter_logs, re-delivered changes included.
-- The row image is `after`, or `before` for a delete (a voided reading).
with source as (
    select record_metadata, record_content
    from {{ source('bronze', 'legacy_hour_meter_logs_cdc_raw') }}
),

events as (
    select
        record_content:op::string                                              as op,
        record_content:source:lsn::number                                      as lsn,
        record_content:source:txId::number                                     as transaction_id,
        to_timestamp_tz(record_content:source:ts_ms::number, 3)                as committed_at,
        to_timestamp_tz(record_content:ts_ms::number, 3)                       as captured_at,
        iff(op = 'd', record_content:before, record_content:after)             as row_image,
        record_metadata:partition::number                                      as kafka_partition,
        record_metadata:offset::number                                         as kafka_offset,
        to_timestamp_tz(record_metadata:SnowflakeConnectorPushTime::number, 3) as ingested_at
    from source
)

select
    {{ dbt_utils.generate_surrogate_key(["lsn", "row_image:log_id::number", "op"]) }} as change_id,
    op,
    decode(op, 'r', 'SNAPSHOT', 'c', 'INSERT', 'u', 'UPDATE', 'd', 'DELETE')        as change_type,
    op = 'd'                                                                       as is_delete,
    lsn,
    transaction_id,
    committed_at,
    captured_at,
    row_image:log_id::number                                                       as log_id,
    row_image:equipment_id::number                                                 as equipment_id,
    row_image:reading_at::timestamp_tz                                             as reading_at,
    -- numeric(9,1) arrives as a decimal string, so no precision is lost on the way.
    row_image:hour_meter::number(9, 1)                                             as hour_meter,
    row_image:source::string                                                       as reading_source,
    row_image:recorded_by::string                                                  as recorded_by,
    row_image:created_at::timestamp_tz                                             as created_at,
    row_image:updated_at::timestamp_tz                                             as updated_at,
    kafka_partition,
    kafka_offset,
    ingested_at
from events
