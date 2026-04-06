-- int_complaints_response_time.sql — Silver Layer
-- What this model does:
--
-- Every 311 complaint has a created_at (when someone called)
-- and a closed_at (when the city resolved it).
--
-- The difference between those two timestamps is the response time.
-- That's what this model calculates.
--
-- But it goes further — it categorises each complaint into
-- response time buckets and flags complaints that took too long.
--
-- After this model, a single row tells you:
-- "A heating complaint in the Bronx took 72 hours to resolve.
--  That puts it in the SLOW category.
--  The average for heating complaints citywide is 48 hours."
--
-- This is the foundation for our equity finding:
-- "Do complaints in low-income boroughs take longer to resolve?"
--
-- Key decisions:
-- 1. We only calculate response time for CLOSED complaints
--    Open complaints have no closed_at — response time is meaningless
-- 2. We cap response time at 365 days — anything longer is likely
--    a data quality issue (complaint never properly closed)
-- 3. We calculate response time in HOURS not days for precision

with complaints as (

    select
        unique_key,
        created_at,
        closed_at,
        complaint_type,
        complaint_descriptor,
        complaint_status,
        borough,
        agency_code,
        agency_name,
        _ingested_at

    from {{ ref('stg_nyc_311') }}

    -- Only complaints with both timestamps can have response time calculated
    where created_at is not null

),

with_response_time as (

    select
        unique_key,
        created_at,
        closed_at,
        complaint_type,
        complaint_descriptor,
        complaint_status,
        borough,
        agency_code,
        agency_name,

        -- Is this complaint resolved?
        case when complaint_status = 'CLOSED'
             then true else false end as is_closed,

        -- Response time in hours
        -- DATEDIFF returns the difference in the specified unit
        -- We only calculate this for closed complaints with valid timestamps
        case
            when complaint_status = 'CLOSED'
             and closed_at is not null
             and closed_at > created_at
             -- Cap at 365 days (8760 hours) to exclude data quality outliers
             and datediff('hour', created_at, closed_at) <= 8760
            then datediff('hour', created_at, closed_at)
            else null
        end as response_time_hours,

        -- Response time in days (rounded) — easier for humans to read
        case
            when complaint_status = 'CLOSED'
             and closed_at is not null
             and closed_at > created_at
             and datediff('hour', created_at, closed_at) <= 8760
            then round(datediff('hour', created_at, closed_at) / 24.0, 1)
            else null
        end as response_time_days,

        _ingested_at

    from complaints

),

-- Add response time categories
categorised as (

    select
        *,

        -- Response speed bucket
        -- These thresholds are based on NYC's published service level targets
        case
            when response_time_hours is null then 'OPEN OR UNKNOWN'
            when response_time_hours <= 4 then 'SAME DAY (≤4hrs)'
            when response_time_hours <= 24 then 'NEXT DAY (4-24hrs)'
            when response_time_hours <= 72 then 'THIS WEEK (1-3 days)'
            when response_time_hours <= 168 then 'SLOW (3-7 days)'
            else 'VERY SLOW (7+ days)'
        end as response_speed_bucket,

        -- Flag complaints that took longer than a week
        case
            when response_time_hours > 168 then true
            else false
        end as is_slow_response

    from with_response_time

)

select * from categorised