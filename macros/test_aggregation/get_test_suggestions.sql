
{% macro get_test_suggestions(
        relation_name,
        sample = false,
        limit = 10000,
        resource_type = "models",
        column_config = {},
        exclude_types = [],
        exclude_cols = [],
        tests = ["uniqueness", "accepted_values", "range", "string_length", "recency"],
        uniqueness_composite_key_length = 1,
        accepted_values_max_cardinality = 5,
        range_stddevs = 0,
        string_length_stddevs = 0,
        recency_stddevs = 1,
        dbt_config = None,
        return_object = false
    ) %}
    {# Run macro for the specific target DB #}
    {% if execute %}
        {% if "uniqueness" in tests %}
            {% set dbt_config = testgen.get_uniqueness_test_suggestions(
                relation_name=relation_name,
                sample=sample,
                limit=limit,
                resource_type=resource_type,
                column_config=column_config,
                exclude_types=exclude_types,
                exclude_cols=exclude_cols,
                composite_key_length=uniqueness_composite_key_length,
                dbt_config=dbt_config
            ) %}
        {% endif %}

        {% if "accepted_values" in tests %}
            {% set dbt_config = testgen.get_accepted_values_test_suggestions(
                relation_name=relation_name,
                sample=sample,
                limit=limit,
                resource_type=resource_type,
                column_config=column_config,
                exclude_types=exclude_types,
                exclude_cols=exclude_cols,
                max_cardinality=accepted_values_max_cardinality,
                dbt_config=dbt_config
            ) %}
        {% endif %}

        {% if "range" in tests %}
            {% set dbt_config = testgen.get_range_test_suggestions(
                relation_name=relation_name,
                sample=sample,
                limit=limit,
                resource_type=resource_type,
                column_config=column_config,
                exclude_types=exclude_types,
                exclude_cols=exclude_cols,
                stddevs=range_stddevs,
                dbt_config=dbt_config
            ) %}
        {% endif %}

        {% if "string_length" in tests %}
            {% set dbt_config = testgen.get_string_length_test_suggestions(
                relation_name=relation_name,
                sample=sample,
                limit=limit,
                resource_type=resource_type,
                column_config=column_config,
                exclude_types=exclude_types,
                exclude_cols=exclude_cols,
                stddevs=string_length_stddevs,
                dbt_config=dbt_config
            ) %}
        {% endif %}

        {% if "recency" in tests %}
            {% set dbt_config = testgen.get_recency_test_suggestions(
                relation_name=relation_name,
                sample=sample,
                limit=limit,
                resource_type=resource_type,
                column_config=column_config,
                exclude_types=exclude_types,
                exclude_cols=exclude_cols,
                stddevs=recency_stddevs,
                dbt_config=dbt_config
            ) %}
        {% endif %}

        {% if return_object %}
            {{ return(dbt_config) }}
        {% else %}
            {% set the_yaml = testgen.to_yaml(dbt_config) %}
            {{ print(the_yaml) }}
            {{ return(the_yaml) }}
        {% endif %}
    {% endif %}
{%- endmacro %}
