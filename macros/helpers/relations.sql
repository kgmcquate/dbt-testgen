
{% macro get_relation_name(relation_name) %}
    -- {% print('here1') %}

    -- {% print(relation_name.identifier) %}
    
    {% if relation_name.identifier | trim == '' %}
        {{ return(relation_name) }}
    {% endif %}

    {{ return(relation_name.identifier) }}
{% endmacro %}


{% macro get_relation(relation_name) %}
    -- {% print('here1') %}

    -- {% print(relation_name.identifier) %}
    
    {% if relation_name.identifier | trim == '' %}
        {{ return(ref(relation_name)) }}
    {% endif %}

    {{ return(relation_name) }}
{% endmacro %}

