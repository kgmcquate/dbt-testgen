{{
    config(alias='sp500_agg_alias')
}}
WITH
formatted as (
    SELECT
        {{ dbt_date.date_part("dayofweek", "day") }} as day_of_week,
        high,
        low
    FROM {{ ref('sp500_daily') }}
)
SELECT
    day_of_week,
    MAX(high) as high,
    MIN(low) as low
FROM formatted
GROUP BY day_of_week