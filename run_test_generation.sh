dbt run-operation print_uniqueness_test_suggestions_from_name \
    --args "{ schema_name='public', table_name='water_bodies' , is_source=true }"
