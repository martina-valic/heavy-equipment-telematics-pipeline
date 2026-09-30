-- Current health of every machine in the fleet, for operations dashboards. Grain: one row per
-- machine (its current dimension version). Rebuilt on every run, as of the run time.
--
-- health_status, first match wins:
--   DECOMMISSIONED  retired in the legacy system
--   CRITICAL        in FAULT right now, fault rate over critical_fault_rate in the window, or service overdue
--   OFFLINE         no telemetry for offline_after_minutes
--   WARNING         service due soon, or out-of-range readings in more than max_anomaly_rate of events
--   HEALTHY         none of the above
-- health_reasons lists every condition that applies, not only the one that set the status.
with equipment as (
    select *
    from {{ ref('dim_equipment') }}
    where is_current
),

last_seen as (
    select
        equipment_id,
        max(event_at)                         as last_telemetry_at,
        max_by(operating_state, event_at)     as last_operating_state
    from {{ ref('telemetry_events') }}
    group by equipment_id
),

recent as (
    select
        equipment_id,
        count(*)                                  as event_count,
        count_if(operating_state = 'OPERATIONAL') as operational_event_count,
        count_if(operating_state = 'FAULT')       as fault_event_count,
        count_if(has_out_of_range_values)         as out_of_range_event_count,
        count_if(has_quality_issue)               as quality_issue_event_count
    from {{ ref('telemetry_events') }}
    where event_at >= dateadd('hour', -{{ var('health_window_hours') }}, current_timestamp())
    group by equipment_id
),

meter as (
    select
        equipment_id,
        max_by(hour_meter, reading_at) as latest_hour_meter,
        max(reading_at)                as latest_hour_meter_at
    from {{ ref('hour_meter_logs') }}
    group by equipment_id
),

last_service as (
    select
        equipment_id,
        max_by(hour_meter, reading_at) as hour_meter_at_last_service,
        max(reading_at)                as last_service_at
    from {{ ref('hour_meter_logs') }}
    where reading_source = 'SERVICE_VISIT'
    group by equipment_id
),

measured as (
    select
        equipment.equipment_key,
        equipment.equipment_id,
        equipment.equipment_serial_number,
        equipment.type_name,
        equipment.category,
        equipment.manufacturer,
        equipment.model,
        equipment.assigned_site,
        equipment.status,
        last_seen.last_telemetry_at,
        last_seen.last_operating_state,
        datediff('minute', last_seen.last_telemetry_at, current_timestamp())       as minutes_since_last_telemetry,
        coalesce(recent.event_count, 0)                                             as window_event_count,
        coalesce(recent.fault_event_count, 0)                                       as window_fault_event_count,
        (recent.fault_event_count / nullif(recent.event_count, 0))::float         as window_fault_rate,
        (recent.operational_event_count / nullif(recent.event_count, 0))::float   as window_utilization_rate,
        (recent.out_of_range_event_count / nullif(recent.event_count, 0))::float  as window_out_of_range_event_rate,
        coalesce(recent.quality_issue_event_count, 0)                               as window_quality_issue_event_count,
        meter.latest_hour_meter,
        meter.latest_hour_meter_at,
        last_service.hour_meter_at_last_service,
        last_service.last_service_at,
        equipment.service_interval_hours,
        (equipment.service_interval_hours
            - (meter.latest_hour_meter - last_service.hour_meter_at_last_service))::number(10, 1)
                                                                                    as hours_until_service_due
    from equipment
    left join last_seen    on last_seen.equipment_id = equipment.equipment_id
    left join recent       on recent.equipment_id = equipment.equipment_id
    left join meter        on meter.equipment_id = equipment.equipment_id
    left join last_service on last_service.equipment_id = equipment.equipment_id
),

assessed as (
    select
        *,
        last_telemetry_at is null
            or minutes_since_last_telemetry > {{ var('offline_after_minutes') }}   as is_offline,
        case
            when hours_until_service_due is null then 'UNKNOWN'
            when hours_until_service_due < 0 then 'OVERDUE'
            when hours_until_service_due < {{ var('service_due_soon_fraction') }} * service_interval_hours then 'DUE_SOON'
            else 'OK'
        end                                                                           as service_status,
        not is_offline and last_operating_state = 'FAULT'                             as is_in_fault,
        coalesce(window_fault_rate > {{ var('critical_fault_rate') }}, false)          as has_high_fault_rate,
        coalesce(window_out_of_range_event_rate > {{ var('max_anomaly_rate') }}, false) as has_frequent_out_of_range
    from measured
)

select
    equipment_key,
    equipment_id,
    equipment_serial_number,
    type_name,
    category,
    manufacturer,
    model,
    assigned_site,
    status,
    case
        when status = 'DECOMMISSIONED' then 'DECOMMISSIONED'
        when is_in_fault or has_high_fault_rate or service_status = 'OVERDUE' then 'CRITICAL'
        when is_offline then 'OFFLINE'
        when service_status = 'DUE_SOON' or has_frequent_out_of_range then 'WARNING'
        else 'HEALTHY'
    end                                                                               as health_status,
    array_to_string(array_construct_compact(
        iff(is_in_fault, 'In fault', null),
        iff(has_high_fault_rate, 'High fault rate', null),
        iff(service_status = 'OVERDUE', 'Service overdue', null),
        iff(is_offline, 'No recent telemetry', null),
        iff(service_status = 'DUE_SOON', 'Service due soon', null),
        iff(has_frequent_out_of_range, 'Frequent out-of-range readings', null)
    ), ', ')                                                                          as health_reasons,
    last_telemetry_at,
    last_operating_state,
    minutes_since_last_telemetry,
    is_offline,
    window_event_count,
    window_fault_event_count,
    window_fault_rate,
    window_utilization_rate,
    window_out_of_range_event_rate,
    window_quality_issue_event_count,
    latest_hour_meter,
    latest_hour_meter_at,
    hour_meter_at_last_service,
    last_service_at,
    service_interval_hours,
    hours_until_service_due,
    service_status,
    current_timestamp()::timestamp_tz                                                 as as_of
from assessed
