{#- Fails (use severity: warn) when the share of rows where the boolean `column_name` is true exceeds
    max_rate. Row-level flags record every anomaly; this test reports when they become unusually
    frequent, which points at a real sensor or pipeline problem rather than background noise.
    Use config.where to limit it to a recent window. -#}
{% test anomaly_rate_at_most(model, column_name, max_rate=none) %}
    {%- set max_rate = max_rate if max_rate is not none else var('max_anomaly_rate') -%}
    with rates as (
        select
            count(*)                                       as row_count,
            count_if({{ column_name }})                    as flagged_count,
            flagged_count / nullif(row_count, 0)           as flagged_rate
        from {{ model }}
    )
    select row_count, flagged_count, flagged_rate, {{ max_rate }} as max_rate
    from rates
    where flagged_rate > {{ max_rate }}
{% endtest %}
