{% macro query_as_list(query) %}
    {% set results = run_query(query) %}
    {{ return(results.rows) }}
{% endmacro %}