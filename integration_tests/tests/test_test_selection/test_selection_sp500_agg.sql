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
  - name: day_of_week
    tests:
    - unique
    - not_null
    - accepted_values:
        values:
        - '1'
        - '2'
        - '3'
        - '4'
        - '5'
    - dbt_utils.accepted_range:
        min_value: 1
        max_value: 5
  - name: high
    tests:
    - unique
    - not_null
    - accepted_values:
        values:
        - '4749.52'
        - '4784.72'
        - '4785.39'
        - '4788.43'
        - '4793.3'
    - dbt_utils.accepted_range:
        min_value: 4749.52
        max_value: 4793.3
  - name: low
    tests:
    - unique
    - not_null
    - accepted_values:
        values:
        - '4546.5'
        - '4546.72'
        - '4551.68'
        - '4565.22'
        - '4574.06'
    - dbt_utils.accepted_range:
        min_value: 4546.5
        max_value: 4574.06
{% endset %}

{{ assert_equal (actual_yaml | trim, expected_yaml | trim) }}
