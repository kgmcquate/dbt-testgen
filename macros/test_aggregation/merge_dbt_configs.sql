

{% macro merge_dbt_configs(dbt_config_1, dbt_config_2) %}
    {% if dbt_config_1 == None %}
        {{ return(dbt_config_2) }}
    {% endif %}

    {% if dbt_config_2 == None %}
        {{ return(dbt_config_1) }}
    {% endif %}

    {% set new_config = {} %}

    {# Want to preserve ordering, so don't use sets #}
    {% set model_types = [] %}
    {% for model_type in dbt_config_1.keys()|list + dbt_config_2.keys()|list %}
        {% if model_type not in model_types %}
            {% do model_types.append(model_type) %}
        {% endif %}
    {% endfor %}

    {% for model_type in model_types %}
        {% do new_config.update({model_type: []}) %}

        {% if model_type not in dbt_config_1.keys() %}
            {% do new_config.update({model_type: dbt_config_2[model_type]}) %}
        {% elif model_type not in dbt_config_2.keys() %}
            {% do new_config.update({model_type: dbt_config_1[model_type]}) %}
        {% else %}
            {% set model_names = [] %}

            {% set config_1_model_lookup = {} %}
            {% for model in dbt_config_1[model_type] %}
                {% do model_names.append(model["name"]) %}
                {% do config_1_model_lookup.update({model["name"]: model}) %}
            {% endfor %}

            {% set config_2_model_lookup = {} %}
            {% for model in dbt_config_2[model_type] %}
                {% if model["name"] not in model_names %}
                    {% do model_names.append(model["name"]) %}
                {% endif %}
                {% do config_2_model_lookup.update({model["name"]: model}) %}
            {% endfor %}

            {% set new_models = [] %}
            {% for model_name in model_names %}
                {% set col_names = [] %}

                {% set config_1_col_lookup = {} %}
                {% for col in config_1_model_lookup[model_name]["columns"] %}
                    {% do col_names.append(col["name"]) %}
                    {% do config_1_col_lookup.update({col["name"]: col}) %}
                {% endfor %}
                
                {% set config_2_col_lookup = {} %}
                {% for col in config_2_model_lookup[model_name]["columns"] %}
                    {% if col["name"] not in col_names %}
                        {% do col_names.append(col["name"]) %}
                    {% endif %}
                    {% do config_2_col_lookup.update({col["name"]: col}) %}
                {% endfor %}

                {% set new_columns = [] %}
                {% for col_name in col_names %}
                    {% if col_name not in config_1_col_lookup.keys() %}
                        {% set col_tests = config_2_col_lookup[col_name]["tests"] %}
                    {% elif col_name not in config_2_col_lookup.keys() %}
                        {% set col_tests = config_1_col_lookup[col_name]["tests"] %}
                    {% else %}
                        {% set col_tests = config_1_col_lookup[col_name]["tests"] + config_2_col_lookup[col_name]["tests"] %}
                    {% endif %}
                    {% do new_columns.append({
                            "name": col_name,
                            "tests": col_tests
                        })
                    %}
                {% endfor %}

                {% set model_tests = [] %}
                {% if "tests" in config_1_model_lookup[model_name].keys() %}
                    {% for model_test in config_1_model_lookup[model_name]["tests"] %}
                        {% do model_tests.append(model_test) %}
                    {% endfor %}
                {% endif %}
                {% if "tests" in config_2_model_lookup[model_name].keys() %}
                    {% for model_test in config_2_model_lookup[model_name]["tests"] %}
                        {% do model_tests.append(model_test) %}
                    {% endfor %}
                {% endif %}

                {% set model = {"name": model_name, "columns": new_columns} %}
                {% if model_tests != [] %}
                    {% do model.update({"tests": model_tests})%}
                {% endif %}
                
                {% do new_models.append(model)%}
            {% endfor %}
        {% do new_config.update({model_type: new_models}) %}
        {% endif %}
    {% endfor %}

    {{ return(new_config) }}
{% endmacro %}

