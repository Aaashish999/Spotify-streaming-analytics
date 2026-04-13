with source as (
    select * from {{ ref('raw_artists') }}
),
cleaned as (
    select
        trim(artist_id)                           as artist_id,
        trim(artist_name)                         as artist_name,
        lower(trim(genre))                        as genre,
        upper(trim(country))                      as country,
        cast(monthly_listeners_millions as float) as monthly_listeners_millions
    from source
    where artist_id is not null
)
select * from cleaned