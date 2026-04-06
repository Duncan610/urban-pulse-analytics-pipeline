-- dim_boroughs.sql — Gold Layer (Dimension)


with census as (

    select
        borough,
        survey_year,
        total_population,
        median_household_income,
        poverty_rate_pct,
        total_housing_units,
        total_workers_commuting,
        income_bracket,
        population_tier,
        _ingested_at

    from {{ ref('stg_census') }}

),

-- Add borough-level rankings so we can say
-- "The Bronx ranks #1 in poverty rate" etc.
ranked as (

    select
        *,

        -- Rank boroughs by income (1 = highest income)
        rank() over (
            order by median_household_income desc
        ) as income_rank,

        -- Rank boroughs by poverty rate (1 = highest poverty)
        rank() over (
            order by poverty_rate_pct desc
        ) as poverty_rank,

        -- Rank boroughs by population (1 = largest)
        rank() over (
            order by total_population desc
        ) as population_rank,

        -- Complaints per housing unit proxy
        -- We'll use this later to normalise complaint volumes
        case
            when total_housing_units > 0
            then round(total_population / total_housing_units, 2)
            else null
        end as avg_household_size

    from census

)

select
    -- Surrogate key for joining
    {{ dbt_utils.generate_surrogate_key(['borough']) }} as neighborhood_key,

    borough,
    survey_year,
    total_population,
    median_household_income,
    poverty_rate_pct,
    total_housing_units,
    total_workers_commuting,
    income_bracket,
    population_tier,
    income_rank,
    poverty_rank,
    population_rank,
    avg_household_size,
    _ingested_at

from ranked