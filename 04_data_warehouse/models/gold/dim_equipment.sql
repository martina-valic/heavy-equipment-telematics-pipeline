-- Equipment dimension, SCD Type 2: one row per version of a machine's master data from the legacy
-- system. Facts join the version that was valid when their event happened (equipment_version_join),
-- so site, status and model are reported as they were at the time.
--
-- The first version of each machine is effective from 1900-01-01, so telemetry recorded before the
-- legacy snapshot still finds a version. Deleted periods have no row, so their facts get the unknown
-- member (equipment_key = '-1').
with history as (
    select *
    from {{ ref('equipment_history') }}
    where not is_deleted
),

versions as (
    select
        *,
        row_number() over (partition by equipment_id order by lsn) as dimension_version_number
    from history
)

select
    equipment_version_id                                                        as equipment_key,
    equipment_id,
    equipment_serial_number,
    type_name,
    category,
    manufacturer,
    model,
    commissioned_on,
    assigned_site,
    status,
    service_interval_hours,
    iff(dimension_version_number = 1, '1900-01-01 00:00:00 +00:00'::timestamp_tz, valid_from) as effective_from,
    coalesce(valid_to, '9999-12-31 00:00:00 +00:00'::timestamp_tz)                           as effective_to,
    is_current
from versions

union all

select
    '-1',
    -1,
    'UNKNOWN',
    'UNKNOWN',
    'UNKNOWN',
    'UNKNOWN',
    'UNKNOWN',
    null,
    'UNKNOWN',
    'UNKNOWN',
    null,
    '1900-01-01 00:00:00 +00:00'::timestamp_tz,
    '9999-12-31 00:00:00 +00:00'::timestamp_tz,
    false
