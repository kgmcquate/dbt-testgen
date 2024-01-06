

{% macro merge_dbt_configs(dbt_config_1, dbt_config_2) %}
    {% if dbt_config_1 == None %}
        {{ return(dbt_config_2) }}
    {% endif %}

    {% if dbt_config_2 == None %}
        {{ return(dbt_config_1) }}
    {% endif %}

    {% set new_config = {} %}

    {# Want to preserve ordering, so don't use sets #}
    {% set resource_types = [] %}
    {# {{ print(dbt_config_1) }} #}
    {% for resource_type in dbt_config_1.keys()|list + dbt_config_2.keys()|list %}
        
        {% if resource_type not in resource_types and resource_type in ["models", "sources", "seeds"] %}
            {# {{ print(resource_type) }} #}
            {% do resource_types.append(resource_type) %}
        {% endif %}
    {% endfor %}

    {% for resource_type in resource_types %}
        {% do new_config.update({resource_type: []}) %}

        {% if resource_type not in dbt_config_1.keys() %}
            {% do new_config.update({resource_type: dbt_config_2[resource_type]}) %}
            
        {% elif resource_type not in dbt_config_2.keys() %}
            {% do new_config.update({resource_type: dbt_config_1[resource_type]}) %}
        {% else %}
            {% set model_names = [] %}

            {% set config_1_model_lookup = {} %}
            {% for model in dbt_config_1[resource_type] %}
                {% do model_names.append(model["name"]) %}
                {% do config_1_model_lookup.update({model["name"]: model}) %}
            {% endfor %}

            {% set config_2_model_lookup = {} %}
            {% for model in dbt_config_2[resource_type] %}
                {% if model["name"] not in model_names %}
                    {% do model_names.append(model["name"]) %}
                {% endif %}
                {% do config_2_model_lookup.update({model["name"]: model}) %}
            {% endfor %}

            {# {{ print(model_names) }} #}

            {# {{ print(config_1_model_lookup) }} #}

            {# {{ print(config_2_model_lookup) }} #}

            {% set new_models = [] %}
            {% for model_name in model_names %}
                {% set model_tests = [] %}
                {% if model_name in config_1_model_lookup.keys() %}
                    {% if "tests" in config_1_model_lookup[model_name].keys() %}
                        {% for model_test in config_1_model_lookup[model_name]["tests"] %}
                            {% do model_tests.append(model_test) %}
                        {% endfor %}
                    {% endif %}
                {% endif %}

                {% if model_name in config_2_model_lookup.keys() %}
                    {% if "tests" in config_2_model_lookup[model_name].keys() %}
                        {% for model_test in config_2_model_lookup[model_name]["tests"] %}
                            {% do model_tests.append(model_test) %}
                        {% endfor %}
                    {% endif %}
                {% endif %}

                {% set model = {"name": model_name} %}

                {% if model_tests != [] %}
                    {% do model.update({"tests": model_tests})%}
                {% endif %}

                {% set col_names = [] %}

                {% set config_1_col_lookup = {} %}
                {% if model_name in config_1_model_lookup.keys() %}
                    {% for col in config_1_model_lookup[model_name]["columns"] %}
                        {% do col_names.append(col["name"]) %}
                        {% do config_1_col_lookup.update({col["name"]: col}) %}
                    {% endfor %}
                {% endif %}
                
                {% set config_2_col_lookup = {} %}
                {% if model_name in config_2_model_lookup.keys() %}
                    {% for col in config_2_model_lookup[model_name]["columns"] %}
                        {% if col["name"] not in col_names %}
                            {% do col_names.append(col["name"]) %}
                        {% endif %}
                        {% do config_2_col_lookup.update({col["name"]: col}) %}
                    {% endfor %}
                {% endif %}

                {% set new_columns = [] %}
                {% for col_name in col_names %}
                    {% set new_column = {
                            "name": col_name
                        }
                    %}
                    {% if col_name not in config_1_col_lookup.keys() %}
                        {% for k, v in config_2_col_lookup[col_name].items() %}
                            {% do new_column.update({k: v}) %}
                        {% endfor %}
                        {% set col_tests = config_2_col_lookup[col_name]["tests"] %}
                    {% elif col_name not in config_2_col_lookup.keys() %}
                        {% for k, v in config_1_col_lookup[col_name].items() %}
                            {% do new_column.update({k: v}) %}
                        {% endfor %}
                        {% set col_tests = config_1_col_lookup[col_name]["tests"] %}
                    {% else %}
                        {% for k, v in config_1_col_lookup[col_name].items()|list + config_2_col_lookup[col_name].items()|list %}
                            {% do new_column.update({k: v}) %}
                        {% endfor %}
                        {% set col_tests = config_1_col_lookup[col_name]["tests"] + config_2_col_lookup[col_name]["tests"] %}
                    {% endif %}

                    {% do new_column.update({"tests": col_tests}) %}

                    {% do new_columns.append(new_column) %}
                {% endfor %}

                {% do model.update({"columns": new_columns})%}
                
                {% do new_models.append(model)%}
            {% endfor %}
        {% do new_config.update({resource_type: new_models}) %}
        {% endif %}
    {% endfor %}

    {# {{ print(new_config) }} #}

    

    {{ return(new_config) }}
{% endmacro %}

