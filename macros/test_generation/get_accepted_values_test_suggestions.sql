{% macro array_agg(colname) %}
     {{ return(adapter.dispatch('array_agg', 'testgen')(colname)) }}
{% endmacro %}

{% macro default__array_agg(colname) %}
    {{ return("array_agg(" ~ adapter.quote(colname) ~ "::VARCHAR)") }}
{% endmacro %}

{% macro redshift__array_agg(colname) %}
    {{ return("split_to_array(listagg(" ~ adapter.quote(colname) ~ "::VARCHAR, '|'), '|') ") }}
{% endmacro %}

{% macro bigquery__array_agg(colname) %}
    {{ return("array_agg(CAST(" ~ adapter.quote(colname) ~ " AS STRING))") }}
{% endmacro %}


{% macro get_accepted_values_test_suggestions(
        table_relation,
        sample = false,
        limit = None,
        resource_type = "models",
        column_config = {},
        exclude_types = ["float"],
        exclude_cols = [],
        tags = ["accepted_values"],
        max_cardinality = 5,
        dbt_config = None
    ) %}
    {# Run macro for the specific target DB #}
    {% if execute %}
        {{ return(
            adapter.dispatch('get_accepted_values_test_suggestions', 'testgen')(
                table_relation, 
                sample, 
                limit, 
                resource_type,
                column_config,
                exclude_types, 
                exclude_cols, 
                tags, 
                max_cardinality, 
                dbt_config,
                **kwargs)
            ) 
        }}
    {% endif%}
{% endmacro %}


{% macro default__get_accepted_values_test_suggestions(
        table_relation,
        sample = false,
        limit = None,
        resource_type = "models",
        column_config = {},
        exclude_types = ["float"],
        exclude_cols = [],
        tags = ["accepted_values"],
        max_cardinality = 5,
        dbt_config = None
    ) 
%}
    {# kwargs is used for test configurations #}
    {# {% set test_config = kwargs %} #}
    {# {% if tags != None %}
        {% do test_config.update({"tags": tags}) %}
    {% endif %} #}

    {% set columns = adapter.get_columns_in_relation(table_relation) %}
    {% set columns = testgen.exclude_column_types(columns, exclude_types) %}
    {% set columns = testgen.exclude_column_names(columns, exclude_cols) %}

    {% if columns|length == 0 %}
        {{ return(dbt_config) }}
    {% endif %}

    {% set count_distinct_exprs = [] %}
    {% for column in columns %}
        {# Use capitals for colnames because of snowflake #}
        {% do count_distinct_exprs.append(
            "
            select " ~ loop.index ~ " AS ORDERING, 
                '" ~ column.column ~ "' AS COLNAME, 
                count(1) as CARDINALITY, " ~ 
                testgen.array_agg(column.column) ~ " AS UNIQUE_VALUES
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
            SELECT * FROM {{ table_relation }}
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

    {% set new_dbt_config = {resource_type: [{"name": table_relation.identifier, "columns": column_tests}]} %}

    {# {{ print(new_dbt_config) }} #}

    {% set merged_dbt_config = testgen.merge_dbt_configs(dbt_config, new_dbt_config) %}

    {% do return(merged_dbt_config) %}

{% endmacro %}
