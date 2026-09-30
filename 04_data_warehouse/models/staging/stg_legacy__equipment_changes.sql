-- One row per Debezium change event for fleet.equipment, re-delivered changes included.
-- The row image is `after`, or `before` for a delete, so a delete still says what the row contained.
-- Types follow the data contract (legacy_fleet_cdc.md): dates are days since 1970-01-01.
with source as (
    select record_metadata, record_content
    from {{ source('bronze', 'legacy_equipment_cdc_raw') }}
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
    -- (lsn, table, primary key, op) identifies a change (data contract, "Duplicates").
    {{ dbt_utils.generate_surrogate_key(["lsn", "row_image:equipment_id::number", "op"]) }} as change_id,
    op,
    decode(op, 'r', 'SNAPSHOT', 'c', 'INSERT', 'u', 'UPDATE', 'd', 'DELETE')              as change_type,
    op = 'd'                                                                             as is_delete,
    lsn,
    transaction_id,
    committed_at,
    captured_at,
    row_image:equipment_id::number                                                       as equipment_id,
    row_image:equipment_serial_number::string                                            as equipment_serial_number,
    row_image:type_name::string                                                          as type_name,
    row_image:category::string                                                           as category,
    row_image:manufacturer::string                                                       as manufacturer,
    row_image:model::string                                                              as model,
    dateadd('day', row_image:commissioned_on::number, '1970-01-01'::date)                as commissioned_on,
    row_image:assigned_site::string                                                      as assigned_site,
    row_image:status::string                                                             as status,
    row_image:service_interval_hours::number                                             as service_interval_hours,
    row_image:created_at::timestamp_tz                                                   as created_at,
    row_image:updated_at::timestamp_tz                                                   as updated_at,
    kafka_partition,
    kafka_offset,
    ingested_at
from events
