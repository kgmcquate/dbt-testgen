
{% macro get_random_function() %}
    {{ return(adapter.dispatch('get_random_function', 'testgen')()) }}
{% endmacro %}

{% macro default__get_random_function(colname) %}
    {{ return("RANDOM") }}
{% endmacro %}

{% macro bigquery__get_random_function(colname) %}
    {{ return("RAND") }}
{% endmacro %}
