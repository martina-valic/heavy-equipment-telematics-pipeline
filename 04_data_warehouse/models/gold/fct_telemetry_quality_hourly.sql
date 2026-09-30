-- Fleet-wide telemetry data quality per hour of event time: what arrived, what Silver kept, and
-- what it flagged. Grain: hour_start.
--
--   delivered_event_count     rows in Bronze (every Kafka delivery)
--   unique_event_count        events in Silver after deduplication on event_id
--   duplicate_delivery_count  deliveries that did not add a new event (DUPLICATE_EVENT, redelivery)
--   contract_violation_count  events the producer sent to the DLQ instead (by failure time)
--
-- Incremental: hours with new Bronze rows are recomputed and replaced.
-- The DLQ staging view is only read in incremental runs, so declare it for dbt's dependency graph:
-- depends_on: {{ ref('stg_telemetry__dlq') }}
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='hour_start',
    on_schema_change='append_new_columns',
) }}

with
{% if is_incremental() %}
changed_hours as (
    select date_trunc('hour', event_at) as hour_start
    from {{ ref('stg_telemetry__events') }}
    where ingested_at > {{ incremental_watermark('last_ingested_at') }}
    union
    select date_trunc('hour', failed_at)
    from {{ ref('stg_telemetry__dlq') }}
    where ingested_at > {{ incremental_watermark('last_ingested_at') }}
),
{% endif %}

deliveries as (
    select
        date_trunc('hour', event_at) as hour_start,
        count(*)                     as delivered_event_count,
        max(ingested_at)             as last_ingested_at
    from {{ ref('stg_telemetry__events') }}
    {% if is_incremental() %}
    where date_trunc('hour', event_at) in (select hour_start from changed_hours)
    {% endif %}
    group by 1
),

events as (
    select
        date_trunc('hour', event_at)           as hour_start,
        count(*)                               as unique_event_count,
        count_if(is_incomplete)                as incomplete_event_count,
        count_if(has_missing_values)           as missing_value_event_count,
        count_if(has_out_of_range_values)      as out_of_range_event_count,
        count_if(not has_quality_issue)        as clean_event_count
    from {{ ref('telemetry_events') }}
    {% if is_incremental() %}
    where date_trunc('hour', event_at) in (select hour_start from changed_hours)
    {% endif %}
    group by 1
),

violations as (
    select
        date_trunc('hour', failed_at) as hour_start,
        count(*)                      as contract_violation_count,
        max(ingested_at)              as last_ingested_at
    from {{ ref('telemetry_contract_violations') }}
    {% if is_incremental() %}
    where date_trunc('hour', failed_at) in (select hour_start from changed_hours)
    {% endif %}
    group by 1
),

hours as (
    select hour_start from deliveries
    union
    select hour_start from violations
)

select
    hours.hour_start,
    to_number(to_char(hours.hour_start, 'YYYYMMDD'))                                  as date_key,
    coalesce(deliveries.delivered_event_count, 0)                                     as delivered_event_count,
    coalesce(events.unique_event_count, 0)                                            as unique_event_count,
    coalesce(deliveries.delivered_event_count, 0) - coalesce(events.unique_event_count, 0) as duplicate_delivery_count,
    coalesce(violations.contract_violation_count, 0)                                  as contract_violation_count,
    coalesce(events.incomplete_event_count, 0)                                        as incomplete_event_count,
    coalesce(events.missing_value_event_count, 0)                                     as missing_value_event_count,
    coalesce(events.out_of_range_event_count, 0)                                      as out_of_range_event_count,
    coalesce(events.clean_event_count, 0)                                             as clean_event_count,
    -- Share of all events produced (kept or rejected) that arrived clean.
    coalesce(events.clean_event_count, 0)
        / nullif(coalesce(events.unique_event_count, 0) + coalesce(violations.contract_violation_count, 0), 0)
        ::float                                                                       as clean_event_rate,
    greatest_ignore_nulls(deliveries.last_ingested_at, violations.last_ingested_at)   as last_ingested_at
from hours
left join deliveries on deliveries.hour_start = hours.hour_start
left join events     on events.hour_start = hours.hour_start
left join violations on violations.hour_start = hours.hour_start
