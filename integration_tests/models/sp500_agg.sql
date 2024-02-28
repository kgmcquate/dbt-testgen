{{
    config(alias='sp500_agg_alias')
}}
WITH
formatted as (
    SELECT
        {{ dbt_date.date_part("month", adapter.quote("day")) }} as month,
        high,
        low
    FROM {{ ref('sp500_daily') }}
)
SELECT
    month,
    MAX(high) as high,
    MIN(low) as low
FROM formatted
GROUP BY month
