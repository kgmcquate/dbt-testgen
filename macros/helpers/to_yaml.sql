{% macro to_yaml(the_obj) %}
{{ return(toyaml(fromjson(tojson(the_obj)))) }}
{% endmacro %}