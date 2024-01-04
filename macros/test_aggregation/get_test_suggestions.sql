
{% macro get_test_suggestions(
        table_relation,
        sample = false,
        limit = None,
        resource_type = "models",
        column_config = {},
        exclude_types = [],
        exclude_cols = [],
        tags = [],
        tests = ["uniqueness", "accepted_values", "range", "string_length", "recency"],
        composite_key_length = 1,
        dbt_config = None,
        return_object = false
    ) %}
    {# Run macro for the specific target DB #}
    {% if execute %}
        {% if "uniqueness" in tests %}
            {% set dbt_config = testgen.get_uniqueness_test_suggestions(
                table_relation=table_relation,
                sample=sample,
                limit=limit,
                resource_type=resource_type,
                column_config=column_config,
                exclude_types=exclude_types,
                exclude_cols=exclude_cols,
                tags=tags,
                composite_key_length=composite_key_length,
                dbt_config=dbt_config
            ) %}
        {% endif %}

        {% if "accepted_values" in tests %}
            {% set dbt_config = testgen.get_accepted_values_test_suggestions(
                table_relation=table_relation,
                sample=sample,
                limit=limit,
                resource_type=resource_type,
                column_config=column_config,
                exclude_types=exclude_types,
                exclude_cols=exclude_cols,
                tags=tags,
                dbt_config=dbt_config
            ) %}
        {% endif %}

        {% if "range" in tests %}
            {% set dbt_config = testgen.get_range_test_suggestions(
                table_relation=table_relation,
                sample=sample,
                limit=limit,
                resource_type=resource_type,
                column_config=column_config,
                exclude_types=exclude_types,
                exclude_cols=exclude_cols,
                tags=tags,
                dbt_config=dbt_config
            ) %}
        {% endif %}

        {% if "string_length" in tests %}
            {% set dbt_config = testgen.get_string_length_test_suggestions(
                table_relation=table_relation,
                sample=sample,
                limit=limit,
                resource_type=resource_type,
                column_config=column_config,
                exclude_types=exclude_types,
                exclude_cols=exclude_cols,
                tags=tags,
                dbt_config=dbt_config
            ) %}
        {% endif %}

        {% if "recency" in tests %}
            {% set dbt_config = testgen.get_recency_test_suggestions(
                table_relation=table_relation,
                sample=sample,
                limit=limit,
                resource_type=resource_type,
                column_config=column_config,
                exclude_types=exclude_types,
                exclude_cols=exclude_cols,
                tags=tags,
                dbt_config=dbt_config
            ) %}
        {% endif %}

        {% if return_object %}
            {{ return(dbt_config) }}
        {% else %}
            {{ print(testgen.to_yaml(dbt_config)) }}
        {% endif %}
    {% endif %}
{%- endmacro %}
