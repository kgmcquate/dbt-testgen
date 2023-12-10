{# {{ codegen.generate_base_model(
    source_name='public',
    table_name='daily_weather',
    materialized='table'
) }} #}

{{ get_columns_cardinality(
        'public',
        'water_bodies'
) }}