{% set input_yaml_1 %}
models:
- name: users
  tests: []
  columns:
  - name: user_id
    tests:
    - unique:
        tags:
        - uniqueness
    - not_null:
        tags:
        - uniqueness
{% endset %}

{% set input_yaml_2 %}
models:
- name: users
  tests: []
  columns:
  - name: user_id
    tests:
    - accepted_values:
        values:
        - active
        - inactive
{% endset %}

{% set actual_yaml = toyaml(
        testgen.merge_dbt_configs(
            fromyaml(input_yaml_1), 
            fromyaml(input_yaml_2)
        ) 
    )
%}


{% set expected_yaml %}
models:
- name: users
  columns:
  - name: user_id
    tests:
    - unique:
        tags:
        - uniqueness
    - not_null:
        tags:
        - uniqueness
    - accepted_values:
        values:
        - active
        - inactive
{% endset %}

{{ assert_equal (actual_yaml | trim, expected_yaml | trim) }}