{#- Use a model's custom schema as is (SILVER, GOLD) instead of dbt's default <target>_<custom>,
    so the Medallion tiers are plain schemas in the TELEMATICS database. -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema | trim }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
