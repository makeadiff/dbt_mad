{% macro clean_prefix(column_name) %}
    regexp_replace({{ column_name }}, '^[A-Z_0-9][A-Z_0-9]+[.]', '')
{% endmacro %}
