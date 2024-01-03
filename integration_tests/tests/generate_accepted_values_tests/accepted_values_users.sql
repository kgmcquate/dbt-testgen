{{ config(
    tags="dataset-users"
) }}

{% set actual_yaml = testgen.to_yaml(
        testgen.get_accepted_values_test_suggestions(
            ref('users')
        )
    )
%}

{% set expected_yaml %}
models:
- name: users
  columns:
  - name: user_status
    tests:
    - accepted_values:
        values:
        - active
        - inactive
{% endset %}

{{ assert_equal (actual_yaml | trim, expected_yaml | trim) }}