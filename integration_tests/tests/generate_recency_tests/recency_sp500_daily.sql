{{ config(
    tags="dataset-sp500_daily"
) }}
{% set actual_yaml = testgen.to_yaml(
        testgen.get_recency_test_suggestions(
            ref('sp500_daily')
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
{% endset %}

{{ assert_equal (actual_yaml | trim, expected_yaml | trim) }}