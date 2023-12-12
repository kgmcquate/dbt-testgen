{# {{ codegen.generate_base_model(
    source_name='public',
    table_name='daily_weather',
    materialized='table'
) }} #}

{{ print_uniqueness_test_suggestions(
    'public',
    'water_bodies',
    enabled=true,
    tags=["a", "b"]

) }}
