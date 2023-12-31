

{% set actual_yaml = toyaml(fromjson(tojson(
        testgen.get_range_test_suggestions(
            ref('users')
        )
    )))
%}

{% set expected_yaml %}
models:
- name: users
  columns:
  - name: user_id
    description: Numeric range test generated by dbt-testgen
    tests:
    - dbt_utils.accepted_range:
        min_value: 1
        max_value: 30
  - name: age
    description: Numeric range test generated by dbt-testgen
    tests:
    - dbt_utils.accepted_range:
        min_value: 22
        max_value: 35
{% endset %}

{{ assert_equal (actual_yaml | trim, expected_yaml | trim) }}