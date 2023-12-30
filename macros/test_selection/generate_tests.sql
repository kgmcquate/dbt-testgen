
{% macro get_test_suggestions(
        table_relation,
        sample = false,
        limit = None,
        is_source = false,
        exclude_types = [],
        exclude_cols = [],
        tags = [],
        dbt_config = None
    ) %}
    {# Run macro for the specific target DB #}
    {% if execute %}

    {# get_accepted_values_test_suggestions(
        table_relation,
        sample = false,
        limit = None,
        is_source = false,
        exclude_types = ["float"],
        exclude_cols = [],
        tags = ["accepted_values"],
        max_cardinality = 5,
        dbt_config = None
    ) %}

    get_range_test_suggestions(
        table_relation,
        sample = false,
        limit = None,
        is_source = false,
        exclude_types = [],
        exclude_cols = [],
        tags = ["range"],
        dbt_config = None
    ) %}

    {% macro get_uniqueness_test_suggestions(
        table_relation,
        sample = false,
        limit = None,
        is_source = false,
        exclude_types = ["float"],
        exclude_cols = [],
        tags = ["uniqueness"],
        compound_key_length = 1,
        dbt_config = None
    ) %} #}

        {% set dbt_config = testgen.get_accepted_values_test_suggestions(
            table_relation=table_relation,
            sample=sample,
            limit=limit,
            is_source=is_source,
            exclude_types=exclude_types,
            exclude_cols=exclude_cols,
            tags=tags,
            dbt_config=dbt_config
        ) %}

        {% set dbt_config = testgen.get_range_test_suggestions(
            table_relation=table_relation,
            sample=sample,
            limit=limit,
            is_source=is_source,
            exclude_types=exclude_types,
            exclude_cols=exclude_cols,
            tags=tags,
            dbt_config=dbt_config
        ) %}
{# 
        {% set dbt_config = testgen.get_uniqueness_test_suggestions(
            table_relation=table_relation,
            sample=sample,
            limit=limit,
            is_source=is_source,
            exclude_types=exclude_types,
            exclude_cols=exclude_cols,
            tags=tags,
            dbt_config=dbt_config
        ) %} #}

        {{ print(dbt_config) }}

        {{ return(dbt_config) }}

    {% endif%}
{%- endmacro %}
