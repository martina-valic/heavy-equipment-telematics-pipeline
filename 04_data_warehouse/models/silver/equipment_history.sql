-- SCD Type 2 history of fleet.equipment, derived from the CDC log rather than dbt snapshots: every
-- committed change is in the log, including ones made between dbt runs, with the complete before
-- and after rows.
--
-- One row per version, ordered by LSN. A change starts a new version only if it changes a business
-- attribute: a re-snapshot, or an update that only bumps updated_at, extends the current version.
-- A delete is a version too (is_deleted), which closes the one before it.
--
-- Rebuilt in full on every run. The log is small, and a full rebuild stays correct when changes
-- arrive out of order, which an incremental SCD2 merge does not.
with changes as (
    select * from {{ ref('legacy_equipment_changes') }}
),

hashed as (
    select
        *,
        {{ dbt_utils.generate_surrogate_key([
            'equipment_serial_number', 'type_name', 'category', 'manufacturer', 'model',
            'commissioned_on', 'assigned_site', 'status', 'service_interval_hours',
        ]) }} as attributes_hash
    from changes
),

compared as (
    select
        *,
        lag(attributes_hash) over (partition by equipment_id order by lsn, kafka_offset) as previous_attributes_hash,
        lag(is_delete) over (partition by equipment_id order by lsn, kafka_offset)       as previous_is_delete
    from hashed
),

versions as (
    select
        *,
        -- The row's own updated_at is when it changed; for a snapshot row source.ts_ms is only the
        -- snapshot time. A delete has no new row, so it takes the commit time.
        iff(is_delete, committed_at, updated_at) as valid_from
    from compared
    where previous_attributes_hash is null
        or is_delete
        or previous_is_delete
        or attributes_hash <> previous_attributes_hash
),

windowed as (
    select
        *,
        lead(valid_from) over (partition by equipment_id order by lsn, kafka_offset) as valid_to,
        row_number() over (partition by equipment_id order by lsn, kafka_offset)     as version_number
    from versions
)

select
    {{ dbt_utils.generate_surrogate_key(['equipment_id', 'lsn']) }} as equipment_version_id,
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
    valid_from,
    valid_to,
    version_number,
    valid_to is null and not is_delete                              as is_current,
    is_delete                                                       as is_deleted,
    change_type,
    lsn,
    committed_at
from windowed
