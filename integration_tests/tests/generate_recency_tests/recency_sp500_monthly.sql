{{ config(
    tags="dataset-sp500_monthly"
) }}
{% set actual_yaml = testgen.to_yaml(
        testgen.get_recency_test_suggestions(
            ref('sp500_monthly')
        )
    )
%}

{% set expected_yaml %}
models:
- name: sp500_monthly
  tests:
  - dbt_utils.recency:
      field: month
      datepart: day
      interval: 31
{% endset %}

{{ assert_equal (actual_yaml | trim, expected_yaml | trim) }}