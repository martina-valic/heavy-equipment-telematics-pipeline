-- Operating time and availability per machine and hour. Grain: hour_start x equipment_key.
-- Each telemetry event stands for telemetry_interval_seconds of machine time in its state, so
-- event counts convert to hours idle, operating and in fault.
--
-- Incremental like fct_equipment_signal_hourly: hours with new events are recomputed and replaced.
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='hour_start',
    on_schema_change='append_new_columns',
) }}

{% set hours_per_event = var('telemetry_interval_seconds') ~ ' / 3600' %}

with
{% if is_incremental() %}
changed_hours as (
    select distinct date_trunc('hour', event_at) as hour_start
    from {{ ref('telemetry_events') }}
    where ingested_at > {{ incremental_watermark('last_ingested_at') }}
),
{% endif %}

events as (
    select *, date_trunc('hour', event_at) as event_hour
    from {{ ref('telemetry_events') }}
    {% if is_incremental() %}
    where event_at >= (select min(hour_start) from changed_hours)
        and date_trunc('hour', event_at) in (select hour_start from changed_hours)
    {% endif %}
),

equipment as (
    select * from {{ ref('dim_equipment') }}
),

keyed as (
    select
        events.*,
        coalesce(equipment.equipment_key, '-1') as equipment_key
    from events
    left join equipment
        on {{ equipment_version_join('equipment', 'events.equipment_id', 'events.event_at') }}
),

aggregated as (
    select
        event_hour,
        equipment_key,
        count(*)                                  as event_count,
        count_if(operating_state = 'IDLE')        as idle_event_count,
        count_if(operating_state = 'OPERATIONAL') as operational_event_count,
        count_if(operating_state = 'FAULT')       as fault_event_count,
        count_if(has_quality_issue)               as quality_issue_event_count,
        min(event_at)                             as first_event_at,
        max(event_at)                             as last_event_at,
        max(ingested_at)                          as last_ingested_at
    from keyed
    group by event_hour, equipment_key
)

select
    {{ dbt_utils.generate_surrogate_key(['event_hour', 'equipment_key']) }} as equipment_hour_key,
    event_hour                                                  as hour_start,
    to_number(to_char(event_hour, 'YYYYMMDD'))                  as date_key,
    equipment_key,
    event_count,
    idle_event_count,
    operational_event_count,
    fault_event_count,
    quality_issue_event_count,
    (idle_event_count * {{ hours_per_event }})::float          as idle_hours,
    (operational_event_count * {{ hours_per_event }})::float   as operating_hours,
    (fault_event_count * {{ hours_per_event }})::float         as fault_hours,
    (operational_event_count / event_count)::float             as utilization_rate,
    (1 - fault_event_count / event_count)::float               as availability_rate,
    first_event_at,
    last_event_at,
    last_ingested_at
from aggregated
