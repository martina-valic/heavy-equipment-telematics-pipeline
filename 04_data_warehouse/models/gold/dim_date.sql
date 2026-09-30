-- Calendar dimension, one row per day. date_key is the YYYYMMDD integer the facts carry.
with days as (
    {{ dbt_utils.date_spine(
        datepart='day',
        start_date="'2015-01-01'::date",
        end_date="'2031-01-01'::date",
    ) }}
)

select
    to_number(to_char(date_day, 'YYYYMMDD'))  as date_key,
    date_day::date                            as date_day,
    year(date_day)                            as year_number,
    quarter(date_day)                         as quarter_number,
    month(date_day)                           as month_number,
    monthname(date_day)                       as month_name,
    weekiso(date_day)                         as iso_week_number,
    dayofweekiso(date_day)                    as iso_day_of_week,
    dayname(date_day)                         as day_name,
    dayofweekiso(date_day) in (6, 7)          as is_weekend,
    date_trunc('week', date_day)::date        as week_start_date,
    date_trunc('month', date_day)::date       as month_start_date
from days
