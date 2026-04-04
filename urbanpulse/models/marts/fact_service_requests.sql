-- fct_311.sql — Gold Layer (Fact Table)

{{
    config(
        materialized='incremental',
        unique_key='unique_key',
        on_schema_change='sync_all_columns'
    )
}}

with complaints as (

    select * from {{ ref('int_311') }}

    {% if is_incremental() %}
        where created_at > (
            select dateadd(
                day,
                -{{ var('incremental_lookback_days', 3) }},
                max(created_at)
            )
            from {{ this }}
        )
    {% endif %}

),

response_times as (

    select
        unique_key,
        response_time_hours,
        response_time_days,
        response_speed_bucket,
        is_closed,
        is_slow_response

    from {{ ref('int_complaints') }}

),

neighborhoods as (

    select
        neighborhood_key,
        borough,
        median_household_income,
        poverty_rate_pct,
        income_bracket,
        population_tier,
        income_rank,
        poverty_rank,
        total_population

    from {{ ref('dim_neighborhood') }}

),

dates as (

    select
        date_day,
        day_of_week_name,
        month_name,
        quarter_name,
        season,
        is_weekend,
        is_us_holiday

    from {{ ref('dim_date') }}

),

-- Bring everything together
final as (

    select
        -- Primary key
        c.unique_key,

        -- Complaint details
        c.created_at,
        c.closed_at,
        c.complaint_date,
        c.complaint_type,
        c.complaint_descriptor,
        c.complaint_status,
        c.borough,
        c.city,
        c.incident_zip,
        c.latitude,
        c.longitude,
        c.agency_code,
        c.agency_name,
        c.resolution_description,

        -- Date dimension attributes
        d.day_of_week_name,
        d.month_name,
        d.quarter_name,
        d.season,
        d.is_weekend,
        d.is_us_holiday,

        -- Weather context
        c.weather_main,
        c.weather_description,
        c.temp_celsius,
        c.feels_like_celsius,
        c.humidity_pct,
        c.rain_1h_mm,
        c.snow_1h_mm,
        c.is_rainy,
        c.is_snowy,
        c.is_cold,
        c.is_hot,
        c.has_weather_data,

        -- Response time
        r.is_closed,
        r.response_time_hours,
        r.response_time_days,
        r.response_speed_bucket,
        r.is_slow_response,

        -- Neighborhood demographics
        n.neighborhood_key,
        n.median_household_income as borough_median_income,
        n.poverty_rate_pct as borough_poverty_rate,
        n.income_bracket as borough_income_bracket,
        n.population_tier as borough_population_tier,
        n.income_rank as borough_income_rank,
        n.poverty_rank as borough_poverty_rank,
        n.total_population as borough_population,

        -- Pipeline metadata
        c._ingested_at

    from complaints c

    -- Join response times
    left join response_times r
        on c.unique_key = r.unique_key

    -- Join neighborhood demographics
    left join neighborhoods n
        on c.borough = n.borough

    -- Join date dimension
    left join dates d
        on c.complaint_date = d.date_day

)

select * from final