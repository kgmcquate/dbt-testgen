{{ config(
    tags="dataset-colnames_with_spaces"
) }}

{% set actual_yaml = testgen.to_yaml(
        testgen.get_uniqueness_test_suggestions(
            ref('colnames_with_spaces'),
            composite_key_length = 1,
            column_config={'quote': true}
        )
    )
%}

{% set expected_yaml %}
models:
- name: colnames_with_spaces
  columns:
  - name: first name
    tests:
    - unique
    - not_null
    quote: true
  - name: age (years)
    tests:
    - unique
    - not_null
    quote: true
  - name: current city
    tests:
    - unique
    - not_null
    quote: true
{% endset %}

{{ assert_equal (actual_yaml | trim, expected_yaml | trim) }}