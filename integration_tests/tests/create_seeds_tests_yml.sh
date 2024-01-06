dbt compile --target ${DBT_TARGET} -q \
    --inline "{{ testgen.get_test_suggestions(ref('users'), resource_type='seeds', column_config={'quote': true, 'tags': ['dataset-users']} ) }}" \
    > seeds/users_test_suggestions.yml

dbt compile --target ${DBT_TARGET} -q \
    --inline "{{ testgen.get_test_suggestions(ref('colnames_with_spaces'), resource_type='seeds', column_config={'quote': true, 'tags': ['dataset-colnames_with_spaces']}) }}"  \
    > seeds/colnames_with_spaces_test_suggestions.yml

dbt compile --target ${DBT_TARGET} -q \
    --inline "{{ testgen.get_test_suggestions(ref('sp500_daily'), resource_type='seeds', column_config={'quote': true, 'tags': ['dataset-sp500_daily']}) }}"  \
    > seeds/sp500_daily_test_suggestions.yml

dbt compile --target ${DBT_TARGET} -q \
    --inline "{{ testgen.get_test_suggestions(ref('sp500_monthly'), resource_type='seeds', column_config={'quote': true, 'tags': ['dataset-sp500_monthly']}) }}"  \
    > seeds/sp500_monthly_test_suggestions.yml