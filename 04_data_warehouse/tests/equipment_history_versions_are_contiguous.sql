-- SCD2 integrity: each version of a machine ends exactly where the next one starts, starts before
-- it ends, and only the latest non-deleted version is current. Returns offending versions.
with versions as (
    select
        *,
        lead(valid_from) over (partition by equipment_id order by version_number) as next_valid_from,
        max(version_number) over (partition by equipment_id)                     as last_version_number
    from {{ ref('equipment_history') }}
)

select equipment_id, version_number, valid_from, valid_to, next_valid_from, is_current, is_deleted
from versions
where valid_to is distinct from next_valid_from
    or valid_to <= valid_from
    or is_current <> (version_number = last_version_number and not is_deleted)
