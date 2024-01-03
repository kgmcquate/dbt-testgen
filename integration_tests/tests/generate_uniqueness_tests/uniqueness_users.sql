{{ config(
    tags="dataset-users"
) }}

{% set actual_yaml = testgen.to_yaml(
        testgen.get_uniqueness_test_suggestions(
            ref('users'),
            composite_key_length = 1
        )
    )
%}

{% set expected_yaml %}
models:
- name: users
  columns:
  - name: user_id
    tests:
    - unique
    - not_null
  - name: username
    tests:
    - unique
    - not_null
  - name: email
    tests:
    - unique
    - not_null
{% endset %}

{{ assert_equal (actual_yaml | trim, expected_yaml | trim) }}