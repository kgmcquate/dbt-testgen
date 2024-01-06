{% set input_yaml_1 %}
version: 2

sources:
  - name: raw_jaffle_shop
    description: A replica of the postgres database used to power the jaffle_shop app.
    tables:
      - name: customers
        columns:
          - name: id
            description: Primary key of the table
            tests:
              - unique
              - not_null

      - name: orders
        columns:
          - name: id
            description: Primary key of the table
            tests:
              - unique
              - not_null

          - name: user_id
            description: Foreign key to customers

          - name: status
            tests:
              - accepted_values:
                  values: ['placed', 'shipped', 'completed', 'return_pending', 'returned']


models:
  - name: stg_jaffle_shop__customers
    config:
      tags: ['pii']
    columns:
      - name: customer_id
        tests:
          - unique
          - not_null

  - name: stg_jaffle_shop__orders
    config:
      materialized: view
    columns:
      - name: order_id
        tests:
          - unique
          - not_null
      - name: status
        tests:
          - accepted_values:
              values: ['placed', 'shipped', 'completed', 'return_pending', 'returned']
              config:
                severity: warn

{% endset %}

{% set input_yaml_2 %}
models:
- name: users
  tests: []
  columns:
  - name: user_id
    tests:
    - accepted_values:
        values:
        - active
        - inactive
{% endset %}

{% set actual_yaml = toyaml(
        testgen.merge_dbt_configs(
            fromyaml(input_yaml_1), 
            fromyaml(input_yaml_2)
        ) 
    )
%}


{% set expected_yaml %}
sources:
- name: raw_jaffle_shop
  description: A replica of the postgres database used to power the jaffle_shop app.
  tables:
  - name: customers
    columns:
    - name: id
      description: Primary key of the table
      tests:
      - unique
      - not_null
  - name: orders
    columns:
    - name: id
      description: Primary key of the table
      tests:
      - unique
      - not_null
    - name: user_id
      description: Foreign key to customers
    - name: status
      tests:
      - accepted_values:
          values:
          - placed
          - shipped
          - completed
          - return_pending
          - returned
models:
- name: stg_jaffle_shop__customers
  columns:
  - name: customer_id
    tests:
    - unique
    - not_null
- name: stg_jaffle_shop__orders
  columns:
  - name: order_id
    tests:
    - unique
    - not_null
  - name: status
    tests:
    - accepted_values:
        values:
        - placed
        - shipped
        - completed
        - return_pending
        - returned
        config:
          severity: warn
- name: users
  columns:
  - name: user_id
    tests:
    - accepted_values:
        values:
        - active
        - inactive
{% endset %}

{{ assert_equal (actual_yaml | trim, expected_yaml | trim) }}