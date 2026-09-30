-- One row per unique telemetry event (deduplicated on event_id), with its data-quality summary
-- rolled up from telemetry_readings. First delivery wins, as in telemetry_readings.
--
-- Built after telemetry_readings: an event is only added once its readings are in Silver, so the
-- quality counts are never computed from a partial set.
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_id',
    merge_update_columns=['event_id'],
    incremental_predicates=[recent_merge_predicate()],
    on_schema_change='append_new_columns',
) }}

with events as (
    select *
    from {{ ref('stg_telemetry__events') }}
    {% if is_incremental() %}
    where ingested_at > {{ incremental_watermark('ingested_at') }}
    {% endif %}
    qualify row_number() over (
        partition by event_id order by ingested_at, kafka_partition, kafka_offset
    ) = 1
),

reading_quality as (
    select
        event_id,
        count(*)                          as reading_count,
        count_if(is_expected_signal)      as expected_signal_reading_count,
        count_if(is_missing_value)        as missing_value_count,
        count_if(is_out_of_range)         as out_of_range_count,
        count_if(not is_expected_signal)  as unexpected_signal_count
    from {{ ref('telemetry_readings') }}
    {% if is_incremental() %}
    where ingested_at > {{ incremental_watermark('ingested_at') }}
    {% endif %}
    group by event_id
),

expected_signals as (
    select type_name, count(*) as expected_reading_count
    from {{ ref('signal_catalog') }}
    group by type_name
)

select
    events.event_id,
    events.schema_version,
    events.event_at,
    events.sequence_number,
    events.equipment_id,
    events.equipment_serial_number,
    events.type_name,
    events.category,
    events.operating_state,
    reading_quality.reading_count,
    expected_signals.expected_reading_count,
    greatest(
        expected_signals.expected_reading_count - reading_quality.expected_signal_reading_count, 0
    )                                                                         as missing_signal_count,
    reading_quality.missing_value_count,
    reading_quality.out_of_range_count,
    reading_quality.unexpected_signal_count,
    missing_signal_count > 0                                                  as is_incomplete,
    reading_quality.missing_value_count > 0                                   as has_missing_values,
    reading_quality.out_of_range_count > 0                                    as has_out_of_range_values,
    is_incomplete or has_missing_values or has_out_of_range_values
        or reading_quality.unexpected_signal_count > 0                        as has_quality_issue,
    events.kafka_topic,
    events.kafka_partition,
    events.kafka_offset,
    events.produced_at,
    events.ingested_at
from events
inner join reading_quality
    on reading_quality.event_id = events.event_id
left join expected_signals
    on expected_signals.type_name = events.type_name
