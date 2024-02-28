-- depends_on: {{ ref('sp500_agg') }}

{{ config(
    tags="dataset-sp500_daily"
) }}

{% set actual_yaml = testgen.to_yaml(
        testgen.get_test_suggestions(
            'sp500_agg',
            return_object=true
        )
    )
%}

{% set expected_yaml %}
models:
- name: sp500_agg
  columns:
  - name: month
    tests:
    - unique
    - not_null
    - accepted_values:
        values:
        - '1'
        - '12'
    - dbt_utils.accepted_range:
        min_value: 1
        max_value: 12
  - name: high
    tests:
    - unique
    - not_null
    - accepted_values:
        values:
        - '4754.33'
        - '4793.3'
    - dbt_utils.accepted_range:
        min_value: 4754.33
        max_value: 4793.3
  - name: low
    tests:
    - unique
    - not_null
    - accepted_values:
        values:
        - '4546.5'
        - '4722.67'
    - dbt_utils.accepted_range:
        min_value: 4546.5
        max_value: 4722.67
{% endset %}

{{ assert_equal (actual_yaml | trim, expected_yaml | trim) }}
