-- =============================================================================
-- stg_census.sql — Bronze Layer
-- =============================================================================
-- Cleans and types raw US Census ACS 5-year estimates.
-- One row per borough per survey year.
--
-- Main jobs here:
--   1. Standardise borough names to uppercase
--   2. Rename columns to be more readable
--   3. Add an income bracket classification per borough
--      (LOW / MEDIUM / HIGH) — used for grouping in Gold layer
-- =============================================================================

with source as (

    select * from {{ source('raw', 'census_raw') }}

),

cleaned as (

    select
        -- Geography
        upper(trim(borough)) as borough,
        trim(county_fips) as county_fips,
        trim(state_fips) as state_fips,
        survey_year,

        -- Demographics
        total_population,
        median_household_income,
        population_below_poverty,
        total_housing_units,
        total_workers_commuting,
        poverty_rate_pct,

        -- Derived: income bracket classification
        -- This lets us group boroughs into tiers for analysis
        -- "Do low-income boroughs wait longer for complaint resolution?"
        case
            when median_household_income < 60000  then 'LOW'
            when median_household_income < 85000  then 'MEDIUM'
            else 'HIGH'
        end as income_bracket,

        -- Derived: population density tier
        -- Manhattan is densest, Staten Island least dense
        case
            when total_population > 2000000 then 'VERY HIGH'
            when total_population > 1500000 then 'HIGH'
            when total_population > 1000000 then 'MEDIUM'
            else 'LOW'
        end as population_tier,

        -- Pipeline metadata
        _ingested_at,
        _source

    from source

    where borough is not null

)

select * from cleaned