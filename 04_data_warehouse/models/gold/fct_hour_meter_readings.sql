-- Hour-meter readings from the legacy system, as they stand now (corrections applied, voided
-- readings removed). Grain: one row per reading (log_id).
with logs as (
    select * from {{ ref('hour_meter_logs') }}
),

equipment as (
    select * from {{ ref('dim_equipment') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['logs.log_id']) }}  as hour_meter_reading_key,
    logs.log_id,
    coalesce(equipment.equipment_key, '-1')                  as equipment_key,
    to_number(to_char(logs.reading_at, 'YYYYMMDD'))          as date_key,
    logs.reading_at,
    logs.hour_meter,
    (logs.hour_meter - lag(logs.hour_meter) over (
        partition by logs.equipment_id order by logs.reading_at, logs.log_id
    ))::number(10, 1)                                        as hours_since_previous_reading,
    logs.reading_source,
    logs.reading_source = 'SERVICE_VISIT'                    as is_service_visit,
    logs.recorded_by,
    logs.correction_count
from logs
left join equipment
    on {{ equipment_version_join('equipment', 'logs.equipment_id', 'logs.reading_at') }}
