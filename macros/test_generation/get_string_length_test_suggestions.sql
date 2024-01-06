
{% macro get_string_length_test_suggestions(
        table_relation,
        sample = false,
        limit = None,
        resource_type = "models",
        column_config = {},
        exclude_types = [],
        exclude_cols = [],
        stddevs = 0,
        dbt_config = None
    ) %}
    {# Run macro for the specific target DB #}
    {% if execute %}
        {{ return(adapter.dispatch('get_string_length_test_suggestions', 'testgen')(table_relation, sample, limit, resource_type, column_config, exclude_types, exclude_cols, stddevs, dbt_config, **kwargs)) }}
    {% endif%}
{%- endmacro %}


{% macro default__get_string_length_test_suggestions(
        table_relation,
        sample = false,
        limit = None,
        resource_type = "models",
        column_config = {},
        exclude_types = [],
        exclude_cols = [],
        stddevs = 0,
        dbt_config = None
    ) 
%}
    {% set columns = adapter.get_columns_in_relation(table_relation) %}
    {% set columns = testgen.exclude_column_types(columns, exclude_types) %}
    {% set columns = testgen.exclude_column_names(columns, exclude_cols) %}

    {# {{ print(columns) }} #}

    {% set string_cols = [] %}
    {% for column in columns %}
        {# {{ print(column.data_type) }} #}
        {% if column.is_string() or column.data_type|lower in ["string"] %}
            {% do string_cols.append(column) %}
        {% endif %}
    {% endfor %}

    {# {{ print(string_cols) }} #}

    {% if string_cols|length == 0 %}
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
    {% for column in string_cols %}
        {% do min_max_exprs.append(
            "SELECT '" ~ column.column ~ "' AS COLNAME, " ~ 
                "MIN(LENGTH(" ~ adapter.quote(column.column) ~ ")) as COL_MIN, " ~ 
                "MAX(LENGTH(" ~ adapter.quote(column.column) ~ ")) as COL_MAX, " ~ 
                "STDDEV(LENGTH(" ~ adapter.quote(column.column) ~ ")) as COL_STDDEV, " ~ 
                loop.index ~ " AS ORDERING " ~ 
            "FROM base 
            WHERE " ~ adapter.quote(column.column) ~ " IS NOT NULL"
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

    {# {{ print(results) }} #}

    {% set column_tests = [] %}
    {% for result in results %}

        {# {{ print(result) }} #}

        {% set min_val = testgen.cast_number(result[1]) %}
        {% set max_val = testgen.cast_number(result[2]) %}
        {% set stddev = testgen.cast_number(result[3]) %}

        {% if min_val == max_val %}
            {% set test = {
                    "dbt_expectations.expect_column_value_lengths_to_equal": {
                        "value": min_val,
                        "row_condition": adapter.quote(result[0]) ~ " is not null"
                    }
                }
            %}
        {% else %}
            {% set min_val = min_val - stddevs*stddev %}
            {% set max_val = max_val + stddevs*stddev %}
            {% if min_val < 0 %}
                {% set min_val = 0 %}
            {% endif %}
            {% set test = {
                    "dbt_expectations.expect_column_value_lengths_to_be_between": {
                        "min_value": testgen.cast_number(min_val),
                        "max_value": testgen.cast_number(max_val),
                        "row_condition": adapter.quote(result[0]) ~ " is not null"
                    }
                }
            %}
        {% endif %}

        {% set col_config = {
                "name": result[0],
                "tests": [test]
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

