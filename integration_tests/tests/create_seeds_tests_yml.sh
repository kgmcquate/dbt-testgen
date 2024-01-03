dbt compile --target ${DBT_TARGET} -q \
    --inline "{{ testgen.get_test_suggestions(ref('users'), resource_type='seeds', column_config={'quote': true} ) }}" \
    > seeds/users_test_suggestions.yml

dbt compile --target ${DBT_TARGET} -q \
    --inline "{{ testgen.get_test_suggestions(ref('colnames_with_spaces'), resource_type='seeds', column_config={'quote': true}) }}"  \
    > seeds/colnames_with_spaces_test_suggestions.yml
