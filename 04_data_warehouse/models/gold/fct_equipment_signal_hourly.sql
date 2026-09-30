-- Sensor statistics per machine, signal and hour. Grain: hour_start x equipment_key x signal_key.
-- equipment_key is the dimension version valid at each reading, so an hour in which a machine's
-- master data changed has one row per version.
--
-- min/max/avg/sum use valid values only: missing and out-of-range readings are counted but
-- excluded, so one 999 degC spike does not skew the hourly average.
--
-- Incremental: every hour that received new readings since the last run is recomputed in full
-- from Silver and replaced (delete+insert on hour_start), so late readings land in the right hour.
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='hour_start',
    on_schema_change='append_new_columns',
) }}

with
{% if is_incremental() %}
changed_hours as (
    select distinct date_trunc('hour', event_at) as hour_start
    from {{ ref('telemetry_readings') }}
    where ingested_at > {{ incremental_watermark('last_ingested_at') }}
),
{% endif %}

readings as (
    select
        *,
        date_trunc('hour', event_at)                                      as reading_hour,
        iff(is_missing_value or is_out_of_range, null, numeric_value)     as valid_value
    from {{ ref('telemetry_readings') }}
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
        readings.*,
        coalesce(equipment.equipment_key, '-1') as equipment_key,
        iff(
            readings.is_expected_signal,
            {{ dbt_utils.generate_surrogate_key(['readings.type_name', 'readings.tag']) }},
            '-1'
        )                                        as signal_key
    from readings
    left join equipment
        on {{ equipment_version_join('equipment', 'readings.equipment_id', 'readings.event_at') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['reading_hour', 'equipment_key', 'signal_key']) }} as equipment_signal_hour_key,
    reading_hour                                    as hour_start,
    to_number(to_char(reading_hour, 'YYYYMMDD'))    as date_key,
    equipment_key,
    signal_key,
    count(*)                                        as sample_count,
    count(valid_value)                              as valid_sample_count,
    count_if(is_missing_value)                      as missing_value_count,
    count_if(is_out_of_range)                       as out_of_range_count,
    min(valid_value)                                as min_value,
    max(valid_value)                                as max_value,
    avg(valid_value)                                as avg_value,
    sum(valid_value)                                as sum_value,
    max(ingested_at)                                as last_ingested_at
from keyed
group by reading_hour, equipment_key, signal_key
