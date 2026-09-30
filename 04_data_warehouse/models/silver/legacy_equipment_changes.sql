-- The fleet.equipment change log, deduplicated: Debezium delivers at least once, so the same change
-- can land in Bronze twice. One row per change_id. equipment_history is rebuilt from this log.
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='change_id',
    merge_update_columns=['change_id'],
    on_schema_change='append_new_columns',
) }}

select *
from {{ ref('stg_legacy__equipment_changes') }}
{% if is_incremental() %}
where ingested_at > {{ incremental_watermark('ingested_at') }}
{% endif %}
qualify row_number() over (partition by change_id order by ingested_at, kafka_offset) = 1
