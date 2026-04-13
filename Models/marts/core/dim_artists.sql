with artists as (
    select * from {{ ref('stg_artists') }}
),
final as (
    select
        artist_id,
        artist_name,
        genre,
        country,
        monthly_listeners_millions,
        case
            when monthly_listeners_millions >= 75 then 'mega'
            when monthly_listeners_millions >= 60 then 'major'
            when monthly_listeners_millions >= 50 then 'established'
            else 'emerging'
        end as artist_tier
    from artists
)
select * from final