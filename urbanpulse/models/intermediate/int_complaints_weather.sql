
-- int_complaints_weather.sql — Silver Layer
-- What this model does (explained simply):
--
-- Imagine you have two lists:
--   List 1: Every 311 complaint — who called, what borough, what date
--   List 2: Weather per borough per day — was it raining? was it cold?
--
-- This model staples those two lists together.
-- For every 311 complaint, we attach what the weather was like
-- in that borough on that day.
--
-- After this model, a single row tells you:
-- "Someone in the Bronx complained about noise on March 15th.
--  That day it was 3°C, raining, and humidity was 78%."
--
-- This is the foundation for our key finding:
-- "Do rainy days produce more complaints?"
--
-- Join logic:
-- We join on borough AND date (not timestamp) because:
-- - 311 complaints have a specific time but weather is daily
-- - We want to match "what was the weather on the DAY of the complaint"
-- - A LEFT JOIN means we keep ALL 311 complaints even if no weather data
--   exists for that day (weather data gaps shouldn't lose complaint records)

with complaints as (

    -- Pull from our cleaned Bronze layer — not raw
    -- This is how dbt builds lineage: model references model
    select
        unique_key,
        created_at,
        closed_at,
        complaint_type,
        complaint_descriptor,
        complaint_status,
        borough,
        city,
        incident_zip,
        incident_address,
        latitude,
        longitude,
        agency_code,
        agency_name,
        resolution_description,
        _ingested_at,

        -- Extract just the date for joining with weather
        date(created_at) as complaint_date

    from {{ ref('stg_nyc_311') }}

    -- Only process records with a valid borough and date
    where created_at is not null
      and borough not in ('UNSPECIFIED', '')

),

weather as (

    select
        borough,
        observation_date,
        weather_main,
        weather_description,
        temp_celsius,
        feels_like_celsius,
        humidity_pct,
        wind_speed_ms,
        cloudiness_pct,
        rain_1h_mm,
        snow_1h_mm,
        is_rainy,
        is_snowy,
        is_cold,
        is_hot

    from {{ ref('stg_weather') }}

),

-- Join complaints with weather on borough + date
-- LEFT JOIN: keep all complaints, even if no weather record exists
enriched as (

    select
        -- Complaint fields
        c.unique_key,
        c.created_at,
        c.closed_at,
        c.complaint_date,
        c.complaint_type,
        c.complaint_descriptor,
        c.complaint_status,
        c.borough,
        c.city,
        c.incident_zip,
        c.incident_address,
        c.latitude,
        c.longitude,
        c.agency_code,
        c.agency_name,
        c.resolution_description,

        -- Weather fields — prefixed with weather_ for clarity
        w.weather_main,
        w.weather_description,
        w.temp_celsius,
        w.feels_like_celsius,
        w.humidity_pct,
        w.wind_speed_ms,
        w.cloudiness_pct,
        w.rain_1h_mm,
        w.snow_1h_mm,

        -- Boolean weather flags — NULL if no weather data, default to FALSE
        coalesce(w.is_rainy, false) as is_rainy,
        coalesce(w.is_snowy, false) as is_snowy,
        coalesce(w.is_cold,  false) as is_cold,
        coalesce(w.is_hot,   false) as is_hot,

        -- Flag to know if weather data was available for this complaint
        case when w.borough is not null
             then true else false end as has_weather_data,

        -- Pipeline metadata
        c._ingested_at

    from complaints c
    left join weather w
        on  c.borough       = w.borough
        and c.complaint_date = w.observation_date

)

select * from enriched