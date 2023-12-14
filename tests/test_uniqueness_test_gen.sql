{# {{ print_uniqueness_test_suggestions(
        source("public", "water_bodies"),
        compound_key_length = 3
)}} #}

{{ print_uniqueness_test_suggestions(
        source("public", "daily_weather"),
        compound_key_length = 2
)}}


SELECT 1 LIMIT 0