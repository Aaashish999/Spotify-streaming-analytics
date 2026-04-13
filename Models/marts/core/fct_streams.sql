with streams as (
    select * from {{ ref('stg_streams') }}
),
users as (
    select user_id from {{ ref('dim_users') }}
),
artists as (
    select artist_id from {{ ref('dim_artists') }}
),
final as (
    select
        s.stream_id,
        s.user_id,
        s.artist_id,
        s.song_id,
        s.song_name,
        s.genre,
        s.device_type,
        s.stream_date,
        s.stream_month,
        s.stream_hour,
        s.duration_seconds,
        s.duration_minutes,
        s.was_skipped,
        case when s.was_skipped = false then 1 else 0 end as is_full_listen,
        current_timestamp() as loaded_at
    from streams s
    left join users u on s.user_id = u.user_id
    left join artists a on s.artist_id = a.artist_id
)
select * from final