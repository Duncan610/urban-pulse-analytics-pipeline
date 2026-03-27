-- dim_date.sql — Gold Layer (Dimension)

with date_spine as (
    select
        dateadd(
            day,
            seq4(),
            '2020-01-01'::date
        ) as date_day
    from table(generator(rowcount => 2557))  -- 7 years * 365.25 days

),

with_attributes as (

    select
        -- Primary key
        date_day,

        -- Date parts
        year(date_day) as year,
        month(date_day) as month_number,
        monthname(date_day) as month_name,
        day(date_day) as day_of_month,
        dayofweek(date_day) as day_of_week_number,
        dayname(date_day) as day_of_week_name,
        weekofyear(date_day) as week_of_year,
        quarter(date_day) as quarter_number,
        dayofyear(date_day) as day_of_year,

        -- Quarter label
        'Q' || quarter(date_day) as quarter_name,

        -- Season (Northern Hemisphere)
        case
            when month(date_day) in (12, 1, 2)  then 'Winter'
            when month(date_day) in (3, 4, 5)   then 'Spring'
            when month(date_day) in (6, 7, 8)   then 'Summer'
            else 'Fall'
        end as season,

        -- Weekend flag
        case
            when dayofweek(date_day) in (1, 7)  then true
            else false
        end as is_weekend,

        -- Weekday flag (inverse of weekend)
        case
            when dayofweek(date_day) in (1, 7)  then false
            else true
        end as is_weekday,

        -- First and last day of month flags
        case
            when date_day = date_trunc('month', date_day)
            then true else false
        end as is_first_day_of_month,

        case
            when date_day = last_day(date_day)
            then true else false
        end as is_last_day_of_month,

        -- US Federal Holidays (approximate — fixed date holidays)
        case
            when (month(date_day) = 1  and day(date_day) = 1)   then 'New Year Day'
            when (month(date_day) = 7  and day(date_day) = 4)   then 'Independence Day'
            when (month(date_day) = 11 and day(date_day) = 11)  then 'Veterans Day'
            when (month(date_day) = 12 and day(date_day) = 25)  then 'Christmas Day'
            else null
        end as us_holiday_name,

        case
            when (month(date_day) = 1  and day(date_day) = 1)
              or (month(date_day) = 7  and day(date_day) = 4)
              or (month(date_day) = 11 and day(date_day) = 11)
              or (month(date_day) = 12 and day(date_day) = 25)
            then true else false
        end as is_us_holiday

    from date_spine

)

select * from with_attributes