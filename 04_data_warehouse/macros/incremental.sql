{#- Lower bound of an incremental batch: the newest `column` value already in this model, minus the
    lookback, so late and re-delivered rows are read again. Merges keep the re-read idempotent. -#}
{% macro incremental_watermark(column) -%}
    (
        select coalesce(
            dateadd('minute', -{{ var('incremental_lookback_minutes') }}, max({{ column }})),
            '1970-01-01'::timestamp_tz
        )
        from {{ this }}
    )
{%- endmacro %}


{#- Merge predicate: only scan target rows ingested within merge_window_days. Keeps each 5-minute
    merge cheap as the table grows. -#}
{% macro recent_merge_predicate(column='ingested_at') -%}
    DBT_INTERNAL_DEST.{{ column }} > dateadd('day', -{{ var('merge_window_days') }}, current_timestamp())
{%- endmacro %}


{#- Point-in-time lookup of the SCD2 equipment dimension: the version valid at `at_column`, or the
    unknown member when there is none. Use with a left join to dim_equipment aliased `alias`. -#}
{% macro equipment_version_join(alias, equipment_id_column, at_column) -%}
    {{ alias }}.equipment_id = {{ equipment_id_column }}
    and {{ at_column }} >= {{ alias }}.effective_from
    and {{ at_column }} < {{ alias }}.effective_to
{%- endmacro %}
