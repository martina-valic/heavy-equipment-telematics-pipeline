-- One row per reading of a unique telemetry event: the readings array flattened, typed and checked
-- against the signal catalog. Anomalies are flagged, never dropped, so nothing is silently lost.
--
-- First delivery wins: a merge match only rewrites the key (a no-op), so a redelivered or reused
-- event_id cannot overwrite readings already in Silver.
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='reading_id',
    merge_update_columns=['reading_id'],
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

readings as (
    select
        events.event_id,
        events.equipment_id,
        events.type_name,
        events.event_at,
        events.operating_state,
        events.ingested_at,
        reading.value:tag::string       as tag,
        reading.value:unit::string      as unit,
        reading.value:subsystem::string as subsystem,
        reading.value:value             as raw_value
    from events,
        lateral flatten(input => events.readings) as reading
),

catalog as (
    select * from {{ ref('signal_catalog') }}
),

typed as (
    select
        readings.*,
        -- A JSON null inside a VARIANT is not SQL NULL: IS_NULL_VALUE catches the sensor's "no report".
        readings.raw_value is null or is_null_value(readings.raw_value)   as is_missing_value,
        iff(readings.unit = 'CODE', null, readings.raw_value::float)      as numeric_value,
        iff(readings.unit = 'CODE', readings.raw_value::string, null)     as code_value,
        catalog.tag is not null                                          as is_expected_signal,
        catalog.min_expected,
        catalog.max_expected,
        catalog.allowed_values
    from readings
    left join catalog
        on catalog.type_name = readings.type_name
        and catalog.tag = readings.tag
)

select
    {{ dbt_utils.generate_surrogate_key(['event_id', 'tag']) }} as reading_id,
    event_id,
    equipment_id,
    type_name,
    event_at,
    operating_state,
    tag,
    replace(tag, 'NS=2;s=', '')                                 as signal_name,
    unit,
    subsystem,
    numeric_value,
    code_value,
    is_missing_value,
    is_expected_signal,
    coalesce(
        not is_missing_value and (
            numeric_value < min_expected
            or numeric_value > max_expected
            or (unit = 'CODE' and not array_contains(code_value::variant, split(allowed_values, '|')))
        ),
        false
    )                                                           as is_out_of_range,
    min_expected,
    max_expected,
    ingested_at
from typed
