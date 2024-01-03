{{ config(
    tags="dataset-users"
) }}

{% set actual_yaml = testgen.to_yaml(
        testgen.get_test_suggestions(
            ref('users'),
            return_object=true
        )
    )
%}

{% set expected_yaml %}
models:
- name: users
  columns:
  - name: user_id
    tests:
    - unique
    - not_null
    - dbt_utils.accepted_range:
        min_value: 1
        max_value: 30
  - name: username
    tests:
    - unique
    - not_null
    - dbt_expectations.expect_column_value_lengths_to_be_between:
        min_value: 8
        max_value: 15
        row_condition: '{{ adapter.quote('username') }} is not null'
  - name: email
    tests:
    - unique
    - not_null
    - dbt_expectations.expect_column_value_lengths_to_be_between:
        min_value: 18
        max_value: 25
        row_condition: '{{ adapter.quote('email') }} is not null'
  - name: user_status
    tests:
    - accepted_values:
        values:
        - active
        - inactive
    - dbt_expectations.expect_column_value_lengths_to_be_between:
        min_value: 6
        max_value: 8
        row_condition: '{{ adapter.quote('user_status') }} is not null'
  - name: age
    tests:
    - dbt_utils.accepted_range:
        min_value: 22
        max_value: 35
{% endset %}

{{ assert_equal (actual_yaml | trim, expected_yaml | trim) }}