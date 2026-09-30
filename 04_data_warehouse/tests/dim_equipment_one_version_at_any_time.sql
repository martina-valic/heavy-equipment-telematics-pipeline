-- A point-in-time join must never match two versions: for each machine, effective windows must not
-- overlap, and exactly one version is current unless the machine was deleted. Returns offenders.
with overlaps as (
    select a.equipment_id, a.equipment_key, b.equipment_key as overlapping_key
    from {{ ref('dim_equipment') }} as a
    inner join {{ ref('dim_equipment') }} as b
        on a.equipment_id = b.equipment_id
        and a.equipment_key < b.equipment_key
        and a.effective_from < b.effective_to
        and b.effective_from < a.effective_to
    where a.equipment_key <> '-1'
),

current_counts as (
    select equipment_id, count_if(is_current) as current_versions
    from {{ ref('dim_equipment') }}
    where equipment_key <> '-1'
    group by equipment_id
),

latest_versions as (
    select equipment_id, is_deleted
    from {{ ref('equipment_history') }}
    qualify row_number() over (partition by equipment_id order by version_number desc) = 1
)

select equipment_id, equipment_key, overlapping_key, null as current_versions
from overlaps
union all
select current_counts.equipment_id, null, null, current_counts.current_versions
from current_counts
inner join latest_versions on latest_versions.equipment_id = current_counts.equipment_id
where current_counts.current_versions <> iff(latest_versions.is_deleted, 0, 1)
