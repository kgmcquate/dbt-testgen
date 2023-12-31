


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
    ) %}
    {# Run macro for the specific target DB #}
    {% if execute %}
        {{ return(
            adapter.dispatch('get_uniqueness_test_suggestions', 'testgen')(table_relation, sample, limit, is_source, exclude_types, exclude_cols, tags, compound_key_length, dbt_config, **kwargs)
        ) }}
    {% endif %}
{% endmacro %}


{% macro default__get_uniqueness_test_suggestions(
        table_relation,
        sample = false,
        limit = None,
        is_source = false,
        exclude_types = ["float"],
        exclude_cols = [],
        tags = ["uniqueness"],
        compound_key_length = 1,
        dbt_config = None
    ) 
%}
    
    {# kwargs is used for test configurations #}
    {% set test_config = kwargs %}
    {# {% if tags != None %}
        {% do test_config.update({"tags": tags}) %}
    {% endif %} #}

    {% if is_source == true %}
        {% set models_or_sources = "sources" %}
    {% else %}
        {% set models_or_sources = "models" %}
    {% endif %}

    {% set columns = adapter.get_columns_in_relation(table_relation) %}

    {% set column_names = [] %}
    {% for col in columns %}
        {% if col.column not in exclude_cols %}
            {% if col.is_string() and "string" not in exclude_types %}
                {% do column_names.append(col.column) %}
            {% elif col.is_numeric() and "numeric" not in exclude_types %}
                {% do column_names.append(col.column) %}
            {% elif col.is_number() and "number" not in exclude_types %}
                {% do column_names.append(col.column) %}
            {% elif col.is_integer() and "integer" not in exclude_types %}
                {% do column_names.append(col.column) %}
            {% elif col.is_float() and "float" not in exclude_types %}
                {% do column_names.append(col.column) %}
            {% elif col.column not in exclude_cols %}
                {% do column_names.append(col.column) %}
            {% endif %}
        {% endif %}
    {% endfor %}

    {% set column_combinations = [] %}
    {% for i in range(compound_key_length) %}
        {% for col_combo in modules.itertools.combinations(column_names, i + 1)%}
            {% do column_combinations.append(col_combo) %}
        {% endfor %}
    {% endfor %}

    {% if limit %}
        {% set limit_expr = "LIMIT " ~ limit|string %}
    {% else %}
        {% set limit_expr = "" %}
    {% endif %}

    {% set count_distinct_exprs = [] %}
    {% for column_combo in column_combinations %}
        {% do count_distinct_exprs.append(
            "SELECT " ~ loop.index ~ " AS ordering, count(1) AS cardinality from (SELECT 1 FROM " ~ table_relation ~ " GROUP BY " ~ column_combo|join(", ") ~ ") t"
        ) %}
    {% endfor %}

    {% set count_distinct_sql %}
    {{ count_distinct_exprs | join("\nUNION ALL\n") }}
    ORDER BY ordering ASC
    {% endset %}

    {% set count_sql %}
        {{ "SELECT count(1) AS table_count FROM " ~ table_relation }} 
    {% endset%}

    {% set table_count = testgen.query_as_list(count_sql)[0].table_count %}

    {% set cardinality_results = zip(column_combinations, testgen.query_as_list(count_distinct_sql)) %}

    {% set unique_keys = [] %}
    {% for cardinality_result in cardinality_results %}
        {% if cardinality_result[1].cardinality == table_count %}
            {% do unique_keys.append(cardinality_result[0]) %}
        {% endif %}
    {% endfor %}


    {# This needs to be refactored and pushed up to the query, so unnecessary distinct counts arent run #}
    {% set deduped_unique_keys = [] %}
    {% for unique_key in unique_keys %}
        {% if unique_key|length == 1 %}
            {% do deduped_unique_keys.append(unique_key) %}
        {% else %}
            {% set permutations = [] %}
            {% for key_length in range(unique_key|length) %}
                {% if key_length > 0: %}
                    {% for perm in modules.itertools.permutations(unique_key, key_length) %}
                        {% do permutations.append(perm) %}
                    {% endfor %}
                {% endif %}
            {% endfor %}

            {% set ns = namespace(already_accounted_for=false) %}
            {% for perm in permutations %}
                {% if perm in deduped_unique_keys %}
                    {% set ns.already_accounted_for = true %}
                {% endif %}
            {% endfor %}

            {% if not ns.already_accounted_for %}
                {% do deduped_unique_keys.append(unique_key) %}
            {% endif %}
        {% endif %}
    {% endfor %}

    {% set column_tests = [] %}
    {% set table_tests = [] %}  
    {% for unique_key in deduped_unique_keys %}
        {% if unique_key|length == 1 %}
            {% set tests = [
                    {"unique": test_config},
                    {"not_null": test_config}
            ] %}

            {% if test_config == {} %}
                {% set tests = ["unique", "not_null"] %}
            {% endif %}

            {% do column_tests.append({
                "name": unique_key[0],
                "description": "Uniqueness test generated by dbt-testgen",
                "tests": tests
            }) %}
        {% else %}

            {% do table_tests.append({
                "dbt_utils.unique_combination_of_columns": {
                    "combination_of_columns": unique_key
                }
            }) %}
        {% endif %}
    {% endfor %}

    {% set model = {"name": table_relation.identifier,  "columns": column_tests} %}
    {% if table_tests != [] %}
        {% do model.update({"tests": table_tests}) %} 
    {% endif %}

    {% set new_dbt_config = {models_or_sources: [model]} %}

    {% set merged_dbt_config = testgen.merge_dbt_configs(dbt_config, new_dbt_config) %}

    {% do return(merged_dbt_config) %}

{% endmacro %}


