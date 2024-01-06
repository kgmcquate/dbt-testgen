{% macro cast_number(number) %}
    {% set number = number|string|float|string %}
    {% if number[-2:] == '.0' %}
        {% set number = number|int %}
    {% else %}
        {% if number|string %}

        {% endif %}
        {% set number = number|float %}
    {% endif %}
    {{ return(number) }}
{% endmacro %}


