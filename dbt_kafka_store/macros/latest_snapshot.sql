{#
    Returns only the rows from the most recent snapshot of a bronze_db_* table.
    Every table in one db_snapshot.py run shares the same _extracted_at value,
    so this gives a consistent "PostgreSQL right now" picture.
#}
{% macro latest_snapshot(relation) %}
    select *
    from {{ relation }}
    where _extracted_at = (select max(_extracted_at) from {{ relation }})
{% endmacro %}