{{ config(
    tags="dataset-users"
) }}

{% set actual_yaml = testgen.to_yaml(
        testgen.get_test_suggestions(
            ref('sp500_daily'),
            return_object=true
        )
    )
%}

{% set expected_yaml %}
models:
- name: sp500_daily
  tests:
  - dbt_utils.recency:
      field: date
      datepart: day
      interval: 2
  columns:
  - name: date
    tests:
    - unique
    - not_null
  - name: close
    tests:
    - unique
    - not_null
    - dbt_utils.accepted_range:
        min_value: 4549.34
        max_value: 4783.35
  - name: open
    tests:
    - unique
    - not_null
    - dbt_utils.accepted_range:
        min_value: 4557.25
        max_value: 4786.44
  - name: high
    tests:
    - unique
    - not_null
    - dbt_utils.accepted_range:
        min_value: 4572.37
        max_value: 4793.3
  - name: low
    tests:
    - unique
    - not_null
    - dbt_utils.accepted_range:
        min_value: 4546.5
        max_value: 4780.98
{% endset %}

{{ assert_equal (actual_yaml | trim, expected_yaml | trim) }}