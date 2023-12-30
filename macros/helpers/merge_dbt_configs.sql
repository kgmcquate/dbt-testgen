{# {% macro merge_dbt_configs(config_1, config_2) %}
    {% if config_1 == None %}
        {{ return(config_2) }}
    {% endif %}

    {% if config_2 == None %}
        {{ return(config_1) }}
    {% endif %}

    {% set dbt_config = {} %}

    {% for model_type in ["source", "model"] %}
        {% if model_type in config_1.keys() and model_type in config_2.keys() %}
        
            {% set config_2_model_names = [] %}
            {% for model in config_2[model_type] %}
                {% do config_2_model_names.append(model["name"]) %}
            {% endfor %}

            {% for model in config_1[model_type] %}
                {% if model["name"] in config_2_model_names %}
                    {% set config_2_model_col_names = [] %}
                    {% for model in config_2[model_type] %}
                        {% do config_2_model_col_names.append(model["name"]) %}
                    {% endfor %}

                    {% set merged_columns = [] %}
                    {% for col in model["columns"] %}
                        {% if col["name"] in %}
                        {% else %}
                        {% endif %}
                    {% endfor %}
                {% else %}
                    {% set merged_columns = model["columns"] %}
                {% endif %}
            {% endfor %}

        {% elif model_type not in config_1.keys() %}
            {% do dbt_config[model_type] = config_2[model_type] %}
        {% elif model_type not in config_2.keys() %}
            {% do dbt_config[model_type] = config_1[model_type] %}
        {% endif %}
        
    {% endfor %}

    {{ return(dbt_config) }}

{% endmacro %} #}

{% macro reformat_dbt_config(dbt_config) %}
    {% set new_config = {} %}
    {% for model_type, models in dbt_config.items() %}
        {% for model in models %}
            {% set model_name = model["name"] %}

            {{ print(model) }}

            {% do new_config.update({model_type: {model_name: {"tests": []} }}) %}

            {{ print(new_config) }}

            {% for col in model["columns"] %}
                {% do new_config.update(
                        [model_type][model["name"]][col["name"]] = {
                        "tests": col["tests"]
                    }
                ) %}
            {% endfor %}
        {% endfor %}
    {% endfor %}
    {{ return(new_config) }}
{% endmacro %}