-- stg_nyc_311.sql — Bronze Layer


with source as (
    
    select * from {{ source('raw', 'nyc_311_raw') }}

),

cleaned as (

    select
        -- Primary identifier
        unique_key,

        -- Dates: cast from raw strings to proper timestamps
        -- TRY_TO_TIMESTAMP safely returns NULL instead of crashing
        -- if the string can't be converted — production-safe casting
        try_to_timestamp(created_date) as created_at,
        try_to_timestamp(closed_date)  as closed_at,

        -- Complaint details — standardise text to uppercase for consistency
        -- This means 'noise' and 'NOISE' and 'Noise' all become 'NOISE'
        upper(trim(complaint_type)) as complaint_type,
        upper(trim(descriptor))     as complaint_descriptor,
        upper(trim(status))         as complaint_status,

        -- Location — standardise borough names
        upper(trim(borough))        as borough,
        upper(trim(city))           as city,
        trim(incident_zip)          as incident_zip,
        trim(incident_address)      as incident_address,

        -- Coordinates: cast from strings to proper floats
        try_to_double(latitude)     as latitude,
        try_to_double(longitude)    as longitude,

        -- Agency
        upper(trim(agency))         as agency_code,
        trim(agency_name)           as agency_name,

        -- Resolution text - just trim whitespace
        trim(resolution_description) as resolution_description,

        -- Pipeline metadata 
        _ingested_at,
        _ingestion_mode,
        _source

    from source

    -- Only keep records with a valid unique key
    -- Records without a unique key are fundamentally broken
    where unique_key is not null
      and trim(unique_key) != ''

)

select * from cleaned