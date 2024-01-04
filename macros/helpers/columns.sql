{% macro exclude_column_types(columns, exclude_types) %}
    {% set filtered_columns = [] %}
    {% for col in columns %}
        {% if col.is_string() and "string" not in exclude_types %}
            {% do filtered_columns.append(col) %}
        {% elif col.is_numeric() and "numeric" not in exclude_types %}
            {% do filtered_columns.append(col) %}
        {% elif col.is_number() and "number" not in exclude_types %}
            {% do filtered_columns.append(col) %}
        {% elif col.is_integer() and "integer" not in exclude_types %}
            {% do filtered_columns.append(col) %}
        {% elif col.is_float() and "float" not in exclude_types %}
            {% do filtered_columns.append(col) %}
        {% elif col.data_type not in exclude_types %}
            {% do filtered_columns.append(col) %}
        {% endif %}
    {% endfor %}
    {{ return(filtered_columns) }}
{% endmacro %}

{% macro exclude_column_names(columns, exclude_cols) %}
    {% set filtered_columns = [] %}
    {% for col in columns %}
        {% if col.column not in exclude_cols %}
            {% do filtered_columns.append(col) %}
        {% endif %}
    {% endfor %}
    {{ return(filtered_columns) }}
{% endmacro %}