-- Current state of fleet.hour_meter_logs, rebuilt from the CDC log: the latest change per log_id,
-- unless that change is a delete (a voided reading), in which case the reading no longer exists.
with changes as (
    select * from {{ ref('legacy_hour_meter_log_changes') }}
),

latest as (
    select
        *,
        count_if(change_type = 'UPDATE') over (partition by log_id) as correction_count
    from changes
    qualify row_number() over (partition by log_id order by lsn desc, kafka_offset desc) = 1
)

select
    log_id,
    equipment_id,
    reading_at,
    hour_meter,
    reading_source,
    recorded_by,
    correction_count,
    created_at,
    updated_at,
    committed_at as last_committed_at,
    lsn          as last_lsn
from latest
where not is_delete
