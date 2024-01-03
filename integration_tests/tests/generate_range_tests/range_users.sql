{{ config(
    tags="dataset-users"
) }}

{% set actual_yaml = testgen.to_yaml(
        testgen.get_range_test_suggestions(
            ref('users')
        )
    )
%}

{% set expected_yaml %}
models:
- name: users
  columns:
  - name: user_id
    tests:
    - dbt_utils.accepted_range:
        min_value: 1
        max_value: 30
  - name: age
    tests:
    - dbt_utils.accepted_range:
        min_value: 22
        max_value: 35
{% endset %}

{{ assert_equal (actual_yaml | trim, expected_yaml | trim) }}