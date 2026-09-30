-- Signal dimension: one row per signal an equipment type reports (the same tag can have a different
-- range or subsystem on different types). Readings of a tag missing from the catalog get the
-- unknown member (signal_key = '-1').
select
    {{ dbt_utils.generate_surrogate_key(['type_name', 'tag']) }} as signal_key,
    type_name,
    tag,
    signal_name,
    unit,
    unit_description,
    subsystem,
    min_expected,
    max_expected,
    allowed_values,
    is_cumulative
from {{ ref('signal_catalog') }}

union all

select '-1', 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', null, null, null, false
