-- Events the producer rejected against the telemetry contract, one row per DLQ record.
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='violation_id',
    merge_update_columns=['violation_id'],
    on_schema_change='append_new_columns',
) }}

select
    violation_id,
    failed_at,
    contract,
    error_count,
    first_error,
    errors,
    event_id,
    equipment_id,
    equipment_serial_number,
    event_at,
    original_payload,
    produced_at,
    ingested_at
from {{ ref('stg_telemetry__dlq') }}
{% if is_incremental() %}
where ingested_at > {{ incremental_watermark('ingested_at') }}
{% endif %}
