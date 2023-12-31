{{ config(
    tags="dataset-colnames_with_spaces"
) }}


{% set actual_yaml = testgen.to_yaml(
        testgen.get_range_test_suggestions(
            ref('colnames_with_spaces')
        )
    )
%}

{% set expected_yaml %}
models:
- name: colnames_with_spaces
  columns:
  - name: age (years)
    tests:
    - dbt_utils.accepted_range:
        min_value: 22
        max_value: 30
{% endset %}

{{ assert_equal (actual_yaml | trim, expected_yaml | trim) }}