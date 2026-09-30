{#- Fails (use severity: warn) when the share of rows whose `column_name` repeats an earlier row exceeds
    max_rate. For at-least-once sources, where some duplicates are expected and Silver removes them. -#}
{% test duplicate_rate_at_most(model, column_name, max_rate=none) %}
    {%- set max_rate = max_rate if max_rate is not none else var('max_anomaly_rate') -%}
    with rates as (
        select
            count(*)                                          as row_count,
            row_count - count(distinct {{ column_name }})     as duplicate_count,
            duplicate_count / nullif(row_count, 0)            as duplicate_rate
        from {{ model }}
    )
    select row_count, duplicate_count, duplicate_rate, {{ max_rate }} as max_rate
    from rates
    where duplicate_rate > {{ max_rate }}
{% endtest %}
