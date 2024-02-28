
{% macro get_recency_test_suggestions(
        relation_name,
        sample = false,
        limit = None,
        resource_type = "models",
        column_config = {},
        exclude_types = [],
        exclude_cols = [],
        stddevs = 1,
        dbt_config = None
    ) %}
    {# Run macro for the specific target DB #}
    {% if execute %}
        {{ return(
            adapter.dispatch('get_recency_test_suggestions', 'testgen')(
                relation_name, 
                sample, 
                limit, 
                resource_type,
                column_config,
                exclude_types, 
                exclude_cols,
                stddevs,
                dbt_config,
                **kwargs)
            ) 
        }}
    {% endif%}
{% endmacro %}


{% macro default__get_recency_test_suggestions(
        relation_name,
        sample = false,
        limit = None,
        resource_type = "models",
        column_config = {},
        exclude_types = [],
        exclude_cols = [],
        stddevs = 1,
        dbt_config = None
    ) 
%}
    {% set relation_name = testgen.get_relation_name(relation_name) %}
    {% set relation = testgen.get_relation(relation_name) %}
    {% set columns = adapter.get_columns_in_relation(relation) %}
    {% set columns = testgen.exclude_column_types(columns, exclude_types) %}
    {% set columns = testgen.exclude_column_names(columns, exclude_cols) %}

    {% set timestamp_cols = [] %}
    {% for col in columns %}
        {% if col.data_type|lower in ["timestamp", "date"] %}
            {% do timestamp_cols.append(col) %}
        {% endif %}
    {% endfor %}

    {% if timestamp_cols|length == 0 %}
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

    {% set timestep_exprs = [] %}
    {% for col in timestamp_cols %}
        {% set col_sql %}
        SELECT 
            MAX(minutes_diff) AS max_minutes_diff,
            AVG(minutes_diff) AS avg_minutes_diff,
            STDDEV(minutes_diff) AS stddev_minutes_diff,
            {{ loop.index }} AS ordering
        FROM (
            SELECT 
                {{ dbt.datediff("LAG(" ~ adapter.quote(col.column) ~ ", 1) OVER(ORDER BY " ~ adapter.quote(col.column) ~ ")", adapter.quote(col.column), "minute") }} AS minutes_diff
            FROM  base
        ) t2
        WHERE minutes_diff <> 0 
        {% endset %}
        {% do timestep_exprs.append(col_sql) %}
    {% endfor %}

    {% set timestep_sql %}
    WITH base AS (
            SELECT * FROM {{ relation }}
            {{ limit_stmt }}
        )
    {{ timestep_exprs | join("\nUNION ALL\n") }}
    ORDER BY ordering ASC
    {% endset %}

    {# {{ print(timestep_sql) }} #}

    {% set recency_results = zip(timestamp_cols, testgen.query_as_list(timestep_sql)) %}

    {% set table_tests = [] %}
    {% for result in recency_results %}
        {% set max_timestep = result[1][0] %}
        {% set avg_timestep = result[1][1] %}
        {% set stddev_timestep = result[1][2] %}

        {% set interval = avg_timestep + (stddev_timestep*stddevs) %}
        
        {% if interval >= 60*24 %}
            {% set datepart = "day" %}
            {% set interval = interval / (60*24) %}
        {% elif interval >= 60 %}
            {% set datepart = "hour" %}
            {% set interval = interval / 60 %}
        {% else %}
            {% set datepart = "minute" %}
        {% endif %}

        {% set test_config = {
            "dbt_utils.recency": {
                "field": result[0].column,
                "datepart": datepart,
                "interval": interval|int
            }
        } %}
        {% do table_tests.append(test_config) %}
    {% endfor %}



    {% set model = {"name": testgen.get_relation_name(relation_name)} %}
    {% if table_tests != [] %}
        {% do model.update({"tests": table_tests}) %} 
    {% endif %}

    {% set new_dbt_config = {resource_type: [model]} %}

    {# {{ print(new_dbt_config) }} #}

    {% set merged_dbt_config = testgen.merge_dbt_configs(dbt_config, new_dbt_config) %}

    {% do return(merged_dbt_config) %}

{% endmacro %}
