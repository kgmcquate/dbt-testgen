
{% set tablename = testgen.get_relation_name(ref('sp500_agg')) %}

{{ assert_equal(tablename, 'sp500_agg_alias') }}