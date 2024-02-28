{{
    config(alias='sp500_agg_alias')
}}
WITH
formatted as (
    SELECT
        {{ dbt_date.date_part("month", adapter.quote("day")) }} as month,
        {{ adapter.quote("high") }} as high,
        {{ adapter.quote("low") }} as low
    FROM {{ ref('sp500_daily') }}
)
SELECT
    {{ adapter.quote("month") }},
    MAX(high) as {{ adapter.quote("high") }},
    MIN(low) as {{ adapter.quote("low") }}
FROM formatted
GROUP BY month
