

{% set actual_yaml = toyaml(fromjson(tojson(
        testgen.get_uniqueness_test_suggestions(
            ref('users'),
            composite_key_length = 1
        )
    )))
%}

{% set expected_yaml %}
models:
- name: users
  columns:
  - name: user_id
    description: Uniqueness test generated by dbt-testgen
    tests:
    - unique
    - not_null
  - name: username
    description: Uniqueness test generated by dbt-testgen
    tests:
    - unique
    - not_null
  - name: email
    description: Uniqueness test generated by dbt-testgen
    tests:
    - unique
    - not_null
{% endset %}

{{ assert_equal (actual_yaml | trim, expected_yaml | trim) }}