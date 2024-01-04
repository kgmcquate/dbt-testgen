
{% macro get_range_test_suggestions(
        table_relation,
        sample = false,
        limit = None,
        resource_type = "models",
        column_config = {},
        exclude_types = [],
        exclude_cols = [],
        tags = ["range"],
        dbt_config = None
    ) %}
    {# Run macro for the specific target DB #}
    {% if execute %}
        {{ return(adapter.dispatch('get_range_test_suggestions', 'testgen')(table_relation, sample, limit, resource_type, column_config, exclude_types, exclude_cols, tags, dbt_config, **kwargs)) }}
    {% endif%}
{%- endmacro %}


{% macro default__get_range_test_suggestions(
        table_relation,
        sample = false,
        limit = None,
        resource_type = "models",
        column_config = {},
        exclude_types = [],
        exclude_cols = [],
        tags = ["range"],
        dbt_config = None
    ) 
%}
    {# kwargs is used for test configurations #}
    {# {% if tags != None %}
        {% do test_config.update({"tags": tags}) %}
    {% endif %} #}

    {% set columns = adapter.get_columns_in_relation(table_relation) %}
    {% set columns = testgen.exclude_column_types(columns, exclude_types) %}
    {% set columns = testgen.exclude_column_names(columns, exclude_cols) %}

    {% set number_cols = [] %}
    {% for column in columns %}
        {% if column.is_number() %}
            {% do number_cols.append(column) %}
        {% endif %}
    {% endfor %}

    {% if number_cols|length == 0 %}
        {{ return(dbt_config) }}
    {% endif %}

    {% if limit != None %}
        {% if sample == true %}
            {% set limit_stmt = "ORDER BY " ~ testgen.get_random_function() ~ "() LIMIT " ~ limit %}
        {% else %}
            {% set limit_stmt = "LIMIT " ~ limit %}
        {% endif %}
    {% else %}
        {% set limit_stmt = "" %}
    {% endif %}

    {% set min_max_exprs = [] %}
    {% for column in number_cols %}
        {% do min_max_exprs.append(
            "SELECT '" ~ column.column ~ "' AS COLNAME, " ~ 
                "MIN(" ~ adapter.quote(column.column) ~ ") as COL_MIN, " ~ 
                "MAX(" ~ adapter.quote(column.column) ~ ") as COL_MAX, " ~ 
                loop.index ~ " AS ORDERING " ~ 
            "FROM base"
        ) %}
    {% endfor %}


    {% set min_max_sql %}
        WITH base AS (
            SELECT * FROM {{ table_relation }}
            {{ limit_stmt }}
        )
        SELECT * FROM (
            {{ min_max_exprs | join("\nUNION ALL\n") }}
        ) t1
        ORDER BY ORDERING ASC
    {% endset %}

    {% set results = testgen.query_as_list(min_max_sql) %}

    {% set column_tests = [] %}
    {% for result in results %}
        {% set col_config = {
                "name": result[0],
                "tests": [
                    {
                        "dbt_utils.accepted_range": {
                            "min_value": testgen.cast_number(result[1]),
                            "max_value": testgen.cast_number(result[2])
                        }
                    }
                ]
            }
        %}

        {% for k,v in column_config.items() %}
            {% do col_config.update({k: v}) %}
        {% endfor %}

        {% do column_tests.append(col_config) %}
    {% endfor %}

    {% set model = {"name": table_relation.identifier,  "columns": column_tests} %}

    {% set new_dbt_config = {resource_type: [model]} %}

    {% set merged_dbt_config = testgen.merge_dbt_configs(dbt_config, new_dbt_config) %}

    {% do return(merged_dbt_config) %}

{% endmacro %}

