{{ config(
    tags="dataset-users"
) }}
{% set actual_yaml = testgen.to_yaml(
        testgen.get_string_length_test_suggestions(
            ref('users'),
            sample=true,
            limit=100
        )
    )
%}

{% set expected_yaml %}
models:
- name: users
  columns:
  - name: username
    tests:
    - dbt_expectations.expect_column_value_lengths_to_be_between:
        min_value: 8
        max_value: 15
        row_condition: '{{ adapter.quote('username') }} is not null'
  - name: email
    tests:
    - dbt_expectations.expect_column_value_lengths_to_be_between:
        min_value: 18
        max_value: 25
        row_condition: '{{ adapter.quote('email') }} is not null'
  - name: user_status
    tests:
    - dbt_expectations.expect_column_value_lengths_to_be_between:
        min_value: 6
        max_value: 8
        row_condition: '{{ adapter.quote('user_status') }} is not null'
{% endset %}

{{ assert_equal (actual_yaml | trim, expected_yaml | trim) }}