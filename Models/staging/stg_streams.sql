with source as (
    select * from {{ ref('raw_streams') }}
),
cleaned as (
    select
        stream_id,
        trim(user_id)                             as user_id,
        trim(artist_id)                           as artist_id,
        trim(song_id)                             as song_id,
        lower(trim(song_name))                    as song_name,
        lower(trim(genre))                        as genre,
        cast(stream_timestamp as timestamp)       as stream_timestamp,
        cast(stream_timestamp as date)            as stream_date,
        date_trunc('month', cast(stream_timestamp as timestamp)) as stream_month,
        extract(hour from cast(stream_timestamp as timestamp))   as stream_hour,
        coalesce(cast(duration_seconds as integer), 0)           as duration_seconds,
        round(coalesce(cast(duration_seconds as float), 0) / 60, 2) as duration_minutes,
        case
            when lower(trim(was_skipped)) = 'true' then true
            else false
        end                                       as was_skipped,
        lower(trim(device_type))                  as device_type
    from source
    where
        stream_id is not null
        and user_id is not null
        and stream_timestamp is not null
)
select * from cleaned