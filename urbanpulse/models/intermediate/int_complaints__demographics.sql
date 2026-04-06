-- int_complaints__demographics.sql — Silver Layer
-- What this model does (explained simply):
--
-- The Census data tells us about each borough:
-- - How many people live there
-- - What the median income is
-- - What the poverty rate is
-- - What income bracket it falls into (LOW/MEDIUM/HIGH)
--
-- This model attaches that demographic context to every 311 complaint.
--
-- After this model, a single row tells you:
-- "Someone in the Bronx complained about heating.
--  The Bronx has a median income of $47,036,
--  a poverty rate of 26.3%, and is classified as LOW income."
--
-- This is the foundation for our equity finding:
-- "Do low-income boroughs generate different complaint patterns?"
-- "Do low-income boroughs wait longer for resolution?"
--
-- Join logic:
-- Simple LEFT JOIN on borough name.
-- Census data has one row per borough — so this join adds exactly
-- 5 columns of demographic context to every complaint.

with complaints as (

    select
        unique_key,
        created_at,
        closed_at,
        complaint_type,
        complaint_descriptor,
        complaint_status,
        borough,
        incident_zip,
        agency_code,
        agency_name,
        _ingested_at,
        date(created_at) as complaint_date

    from {{ ref('stg_nyc__311') }}

    where created_at is not null
      and borough not in ('UNSPECIFIED', '')

),

demographics as (

    select
        borough,
        survey_year,
        total_population,
        median_household_income,
        poverty_rate_pct,
        total_housing_units,
        income_bracket,
        population_tier

    from {{ ref('stg_census') }}

),

-- Attach borough demographics to every complaint
enriched as (

    select
        -- Complaint identifiers
        c.unique_key,
        c.created_at,
        c.closed_at,
        c.complaint_date,
        c.complaint_type,
        c.complaint_descriptor,
        c.complaint_status,
        c.borough,
        c.incident_zip,
        c.agency_code,
        c.agency_name,

        -- Borough demographic context
        d.survey_year as census_survey_year,
        d.total_population as borough_population,
        d.median_household_income as borough_median_income,
        d.poverty_rate_pct as borough_poverty_rate,
        d.total_housing_units as borough_housing_units,
        d.income_bracket as borough_income_bracket,
        d.population_tier as borough_population_tier,

        -- Derived: complaints per capita proxy
        -- Useful for normalizing complaint volume by population size
        -- "The Bronx has more complaints — but it also has more people"
        case
            when d.total_population > 0
            then 1.0 / d.total_population
            else null
        end as per_capita_weight,

        -- Flag: was demographic data available?
        case when d.borough is not null
             then true else false end as has_demographic_data,

        c._ingested_at

    from complaints c
    left join demographics d
        on c.borough = d.borough

)

select * from enriched