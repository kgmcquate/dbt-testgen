{% macro sql_agg_array(colname) %}
     {{ return(adapter.dispatch('sql_agg_array', 'testgen')(colname)) }}
{% endmacro %}

{% macro default__sql_agg_array(colname) %}
    {{ return("array_agg(" ~ adapter.quote(colname) ~ "::VARCHAR)") }}
{% endmacro %}

{% macro redshift__sql_agg_array(colname) %}
    {{ return("split_to_array(listagg(" ~ adapter.quote(colname) ~ "::VARCHAR, '|'), '|') ") }}
{% endmacro %}

{% macro bigquery__sql_agg_array(colname) %}
    {{ return("array_agg(CAST(" ~ adapter.quote(colname) ~ " AS STRING))") }}
{% endmacro %}

{% macro databricks__sql_agg_array(colname) %}
    {{ return("to_json(array_agg(CAST(" ~ adapter.quote(colname) ~ " AS STRING)))") }}
{% endmacro %}


{% macro get_accepted_values_test_suggestions(
        relation_name,
        sample = false,
        limit = None,
        resource_type = "models",
        column_config = {},
        exclude_types = ["float"],
        exclude_cols = [],
        max_cardinality = 5,
        dbt_config = None
    ) %}
    {# Run macro for the specific target DB #}
    {% if execute %}
        {{ return(
            adapter.dispatch('get_accepted_values_test_suggestions', 'testgen')(
                relation_name, 
                sample, 
                limit, 
                resource_type,
                column_config,
                exclude_types, 
                exclude_cols, 
                max_cardinality, 
                dbt_config,
                **kwargs)
            ) 
        }}
    {% endif%}
{% endmacro %}


{% macro default__get_accepted_values_test_suggestions(
        relation_name,
        sample = false,
        limit = None,
        resource_type = "models",
        column_config = {},
        exclude_types = ["float"],
        exclude_cols = [],
        max_cardinality = 5,
        dbt_config = None
    ) 
%}
    {% set relation_name = testgen.get_relation_name(relation_name) %}
    {# {{ print(relation_name) }} #}
    {% set relation = testgen.get_relation(relation_name) %}
    {# {{ print(relation) }} #}
    {% set columns = adapter.get_columns_in_relation(relation) %}
    {% set columns = testgen.exclude_column_types(columns, exclude_types) %}
    {% set columns = testgen.exclude_column_names(columns, exclude_cols) %}

    {# {{ print(columns) }} #}
    {% if columns|length == 0 %}
        {{ return(dbt_config) }}
    {% endif %}

    {# {{ print(columns) }} #}

    {% set count_distinct_exprs = [] %}
    {% for column in columns %}
        {# Use capitals for colnames because of snowflake #}
        {% do count_distinct_exprs.append(
            "
            select " ~ loop.index ~ " AS ORDERING, 
                '" ~ column.column ~ "' AS COLNAME, 
                count(1) as CARDINALITY, " ~ 
                testgen.sql_agg_array(column.column) ~ " AS UNIQUE_VALUES
            from (
                select " ~ adapter.quote(column.column) ~ "
                from base
                group by " ~ adapter.quote(column.column) ~ "
            ) t1
            "
        ) %}
    {% endfor %}

    {% if limit != None %}
        {% if sample == true %}
            {% set limit_stmt = "ORDER BY " ~ testgen.get_random_function() ~ "() LIMIT " ~ limit %}
        {% else %}
            {% set limit_stmt = "LIMIT " ~ limit %}
        {% endif %}
    {% else %}
        {% set limit_stmt = "" %}
    {% endif %}

    {% set count_distinct_sql %}
        WITH base AS (
            SELECT * FROM {{ relation }}
            {{ limit_stmt }}
        )
        SELECT * FROM (
            {{ count_distinct_exprs | join("\nUNION ALL\n") }}
        ) t2
        WHERE CARDINALITY <= {{ max_cardinality|string }}
        ORDER BY ORDERING ASC
    {% endset %}

    {# {{ print(count_distinct_sql) }} #}

    {% set cardinality_results = testgen.query_as_list(count_distinct_sql) %}

    {# {{ print(cardinality_results) }} #}

    {% set column_tests = [] %}
    {% for cardinality_result in cardinality_results %}

        {# {{ print(cardinality_result.COLNAME) }} #}

        {% set col_config = {
                "name": cardinality_result[1],
                "tests": [
                    {"accepted_values": {"values": fromjson(cardinality_result[3])|sort}}
                ]
            }
        %}

        {% for k,v in column_config.items() %}
            {% do col_config.update({k: v}) %}
        {% endfor %}

        {% do column_tests.append(col_config) %}
    {% endfor %}

    {% set new_dbt_config = {resource_type: [{"name": testgen.get_relation_name(relation_name), "columns": column_tests}]} %}

    {# {{ print(new_dbt_config) }} #}

    {% set merged_dbt_config = testgen.merge_dbt_configs(dbt_config, new_dbt_config) %}

    {% do return(merged_dbt_config) %}

{% endmacro %}
