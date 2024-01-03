{{ config(
    tags="dataset-colnames_with_spaces"
) }}


{% set actual_yaml = testgen.to_yaml(
        testgen.get_string_length_test_suggestions(
            ref('colnames_with_spaces'),
            sample=true,
            limit=100
        )
    )
%}

{% set expected_yaml %}
models:
- name: colnames_with_spaces
  columns:
  - name: first name
    tests:
    - dbt_expectations.expect_column_value_lengths_to_be_between:
        min_value: 3
        max_value: 5
        row_condition: '{{ adapter.quote('first name') }} is not null'
  - name: current city
    tests:
    - dbt_expectations.expect_column_value_lengths_to_be_between:
        min_value: 7
        max_value: 13
        row_condition: '{{ adapter.quote('current city') }} is not null'
{% endset %}

{{ assert_equal (actual_yaml | trim, expected_yaml | trim) }}