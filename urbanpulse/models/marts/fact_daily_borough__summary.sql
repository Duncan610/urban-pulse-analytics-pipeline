<<<<<<< HEAD:urbanpulse/models/marts/fact_daily_borough_summary.sql
-- fact_daily_borough_summary.sql — Gold Layer (Fact Table)
=======
-- fact_daily_borough__summary.sql — Gold Layer (Fact Table)
>>>>>>> 1f8b8ec (Changed file names):urbanpulse/models/marts/fact_daily_borough__summary.sql

with daily_complaints as (

    select
        complaint_date,
        borough,
        borough_income_bracket,
        borough_median_income,
        borough_poverty_rate,

        -- Weather for the day
        weather_main,
        temp_celsius,
        humidity_pct,
        rain_1h_mm,
        is_rainy,
        is_snowy,
        is_cold,
        is_hot,

        -- Date attributes
        day_of_week_name,
        month_name,
        season,
        is_weekend,

        -- Complaint details for aggregation
        unique_key,
        complaint_type,
        is_closed,
        response_time_hours,
        is_slow_response

    from {{ ref('fact_service__requests') }}

    where complaint_date is not null
      and borough is not null

),

aggregated as (

    select
        -- Grain: one row per borough per day
        complaint_date,
        borough,
        borough_income_bracket,
        borough_median_income,
        borough_poverty_rate,

        -- Weather context for the day
        -- MAX because all complaints in same borough+day share same weather
        max(weather_main) as weather_condition,
        max(temp_celsius) as temp_celsius,
        max(humidity_pct) as humidity_pct,
        max(rain_1h_mm) as rain_mm,
        max(is_rainy::int)::boolean as is_rainy,
        max(is_snowy::int)::boolean as is_snowy,
        max(is_cold::int)::boolean as is_cold,
        max(is_hot::int)::boolean as is_hot,

        -- Date attributes
        max(day_of_week_name) as day_of_week,
        max(month_name) as month_name,
        max(season) as season,
        max(is_weekend::int)::boolean as is_weekend,

        -- Complaint volume metrics
        count(unique_key) as total_complaints,
        count(case when is_closed then unique_key end)
                                        as closed_complaints,
        count(case when not is_closed then unique_key end)
                                        as open_complaints,

        -- Top complaint types (for filtering in dashboard)
        count(case when complaint_type = 'NOISE - RESIDENTIAL'
                   then unique_key end) as noise_complaints,
        count(case when complaint_type like '%HEAT%'
                   then unique_key end) as heat_complaints,
        count(case when complaint_type like '%RODENT%'
                   then unique_key end) as rodent_complaints,

        -- Response time metrics
        round(avg(response_time_hours), 1)
                                        as avg_response_time_hours,
        round(avg(case when is_slow_response
                       then response_time_hours end), 1)
                                        as avg_slow_response_hours,
        count(case when is_slow_response then unique_key end)
                                        as slow_response_count

    from daily_complaints
    group by
        complaint_date,
        borough,
        borough_income_bracket,
        borough_median_income,
        borough_poverty_rate

)

select * from aggregated