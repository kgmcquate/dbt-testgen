

{% set actual_yaml = toyaml(fromjson(tojson(
        testgen.get_accepted_values_test_suggestions(
            ref('users')
        )
    )))
%}

{% set expected_yaml %}
models:
- name: users
  columns:
  - name: user_status
    description: Accepted values test generated by dbt-testgen
    tests:
    - accepted_values:
        values:
        - active
        - inactive
{% endset %}

{{ assert_equal (actual_yaml | trim, expected_yaml | trim) }}