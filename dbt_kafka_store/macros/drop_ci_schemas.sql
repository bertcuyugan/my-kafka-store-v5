{#
    Drops the temporary schemas a CI run created for one pull request:
        <catalog>.<ci schema>, <ci schema>_staging, <ci schema>_marts

    Safety: refuses to run unless the target schema starts with "ci_",
    so it can never drop production (kafka_store_v5_dbt_*).

    Usage (CI only):  python run_dbt.py run-operation drop_ci_schemas
#}
{% macro drop_ci_schemas() %}

    {% if not target.schema.startswith('ci_') %}
        {{ exceptions.raise_compiler_error(
            "Refusing to drop schemas: target schema '" ~ target.schema ~ "' does not start with 'ci_'"
        ) }}
    {% endif %}

    {% for suffix in ['', '_staging', '_marts'] %}
        {% set schema_name = target.schema ~ suffix %}
        {% do run_query("DROP SCHEMA IF EXISTS `" ~ target.database ~ "`.`" ~ schema_name ~ "` CASCADE") %}
        {{ log("Dropped " ~ target.database ~ "." ~ schema_name, info=True) }}
    {% endfor %}

{% endmacro %}