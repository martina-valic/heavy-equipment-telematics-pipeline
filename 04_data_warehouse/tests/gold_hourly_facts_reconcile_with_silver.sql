-- The incremental hourly facts must add up to Silver: per hour, the events in fct_equipment_hourly
-- and the readings in fct_equipment_signal_hourly equal what Silver holds. Checks the last 7 days
-- of complete hours. A mismatch means an incremental run skipped or double-counted an hour.
{% set since = "date_trunc('hour', dateadd('day', -7, current_timestamp()))" %}

with silver_events as (
    select date_trunc('hour', event_at) as hour_start, count(*) as silver_count
    from {{ ref('telemetry_events') }}
    where event_at >= {{ since }}
    group by 1
),

gold_events as (
    select hour_start, sum(event_count) as gold_count
    from {{ ref('fct_equipment_hourly') }}
    where hour_start >= {{ since }}
    group by 1
),

silver_readings as (
    select date_trunc('hour', event_at) as hour_start, count(*) as silver_count
    from {{ ref('telemetry_readings') }}
    where event_at >= {{ since }}
    group by 1
),

gold_readings as (
    select hour_start, sum(sample_count) as gold_count
    from {{ ref('fct_equipment_signal_hourly') }}
    where hour_start >= {{ since }}
    group by 1
)

select 'fct_equipment_hourly' as fact, coalesce(s.hour_start, g.hour_start) as hour_start, s.silver_count, g.gold_count
from silver_events as s
full outer join gold_events as g on g.hour_start = s.hour_start
where s.silver_count is distinct from g.gold_count

union all

select 'fct_equipment_signal_hourly', coalesce(s.hour_start, g.hour_start), s.silver_count, g.gold_count
from silver_readings as s
full outer join gold_readings as g on g.hour_start = s.hour_start
where s.silver_count is distinct from g.gold_count
