-- stg_weather.sql — Bronze Layer
-- Cleans and types raw weather observations from OpenWeather API.
-- One row per borough per observation timestamp.
--
-- Key cleaning steps:
--   1. Cast observation_timestamp to proper timestamp
--   2. Standardise borough names to uppercase
--   3. Create a simple date column (we'll join on this later)
--   4. Add a boolean flag: is_rainy — makes downstream logic simpler
--   5. Add a boolean flag: is_cold (below 10°C)
--   6. Add a boolean flag: is_hot (above 30°C)

with source as (

    select * from {{ source('raw', 'weather_raw') }}

),

cleaned as (

    select
        -- Location
        upper(trim(borough)) as borough,
        trim(city_name) as city_name,
        lat as latitude,
        lon as longitude,

        -- Weather condition
        trim(weather_main) as weather_main,
        trim(weather_description) as weather_description,

        -- Temperature
        temp_celsius,
        feels_like_celsius,
        temp_min_celsius,
        temp_max_celsius,

        -- Atmospheric conditions
        pressure_hpa,
        humidity_pct,
        visibility_meters,
        wind_speed_ms,
        cloudiness_pct,

        -- Precipitation — coalesce NULL to 0
        -- NULL means "no rain recorded" which is functionally the same as 0mm
        coalesce(rain_1h_mm, 0) as rain_1h_mm,
        coalesce(snow_1h_mm, 0) as snow_1h_mm,

        -- Timestamps — already TIMESTAMP_NTZ in Snowflake, no casting needed
        observation_timestamp as observed_at,
        sunrise_timestamp as sunrise_at,
        sunset_timestamp as sunset_at,

        -- Derived date column for joining with 311 data
        date(observation_timestamp) as observation_date,

        -- Derived boolean flags — makes downstream models cleaner
        -- Instead of repeating "WHERE rain_1h_mm > 0" everywhere,
        -- we define it once here and reference is_rainy downstream
        case when coalesce(rain_1h_mm, 0) > 0
             then true else false end as is_rainy,

        case when coalesce(snow_1h_mm, 0) > 0
             then true else false end as is_snowy,

        case when temp_celsius < 10
             then true else false end as is_cold,

        case when temp_celsius > 30
             then true else false end as is_hot,

        -- Pipeline metadata
        _ingested_at,
        _source

    from source

    where borough is not null

)

select * from cleaned