

{% set actual_yaml = toyaml(fromjson(tojson(
        testgen.get_test_suggestions(
            ref('users')
        )
    )))
%}

{% set expected_yaml %}
models:
- name: users
  columns:
  - name: user_id
    tests:
    - unique
    - not_null
    - dbt_utils.accepted_range:
        min_value: 1
        max_value: 30
  - name: username
    tests:
    - unique
    - not_null
  - name: email
    tests:
    - unique
    - not_null
  - name: user_status
    tests:
    - accepted_values:
        values:
        - active
        - inactive
  - name: age
    tests:
    - dbt_utils.accepted_range:
        min_value: 22
        max_value: 35
{% endset %}

{{ assert_equal (actual_yaml | trim, expected_yaml | trim) }}