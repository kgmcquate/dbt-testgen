
{# {{ codegen.generate_source('public') }} #}

{{ config(materialized='table') }}

with source as (

    select * from {{ source('public', 'daily_weather') }}

),

renamed as (

    select
        date,
        latitude,
        longitude,
        timezone,
        temperature_2m_max,
        temperature_2m_min,
        sunrise,
        sunset,
        uv_index_max,
        uv_index_clear_sky_max,
        precipitation_sum,
        rain_sum,
        showers_sum,
        snowfall_sum,
        precipitation_hours,
        precipitation_probability_max,
        windspeed_10m_max,
        windgusts_10m_max,
        winddirection_10m_dominant,
        shortwave_radiation_sum,
        et0_fao_evapotranspiration

    from source

)