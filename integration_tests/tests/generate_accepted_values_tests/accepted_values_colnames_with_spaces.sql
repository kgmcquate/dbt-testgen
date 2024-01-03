{{ config(
    tags="dataset-colnames_with_spaces"
) }}

{% set actual_yaml = testgen.to_yaml(
        testgen.get_accepted_values_test_suggestions(
            ref('colnames_with_spaces')
        )
    )
%}

{% set expected_yaml %}
models:
- name: colnames_with_spaces
  columns:
  - name: first name
    tests:
    - accepted_values:
        values:
        - Alice
        - Bob
        - John
  - name: age (years)
    tests:
    - accepted_values:
        values:
        - '22'
        - '25'
        - '30'
  - name: current city
    tests:
    - accepted_values:
        values:
        - Chicago
        - New York
        - San Francisco
{% endset %}

{{ assert_equal (actual_yaml | trim, expected_yaml | trim) }}