{# {{ codegen.generate_base_model(
    source_name='public',
    table_name='daily_weather',
    materialized='table'
) }} #}

{{ print_uniqueness_test_suggestions(
    source('public', 'water_bodies'),
    compound_key_length = 2,
    is_source = true,
    enabled=true,
    tags=["a", "b"]
) }}

{# {{ print_uniqueness_test_suggestions_from_name(
    'public', 'water_bodies',
    enabled=true,
    tags=["a", "b"]
) }} #}

