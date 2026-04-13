with streams as (
    select * from {{ ref('fct_streams') }}
),
users as (
    select * from {{ ref('dim_users') }}
),
final as (
    select
        u.user_id,
        u.user_name,
        u.country,
        u.subscription_type,
        u.age_bucket,
        u.user_tenure,
        count(*)                                         as total_streams,
        count(distinct s.song_id)                        as unique_songs,
        count(distinct s.artist_id)                      as unique_artists,
        sum(s.duration_minutes)                          as total_minutes_streamed,
        round(avg(s.duration_minutes), 2)                as avg_minutes_per_stream,
        sum(s.is_full_listen)                            as full_listens,
        round(100.0 * sum(s.is_full_listen) / count(*), 1) as completion_rate_pct,
        min(s.stream_date)                               as first_stream_date,
        max(s.stream_date)                               as last_stream_date,
        count(distinct s.stream_date)                    as active_days,
        case
            when sum(s.duration_minutes) >= 200 then 'power_listener'
            when sum(s.duration_minutes) >= 100 then 'regular_listener'
            when sum(s.duration_minutes) >= 30  then 'casual_listener'
            else 'light_listener'
        end                                              as listener_tier
    from streams s
    left join users u on s.user_id = u.user_id
    group by 1, 2, 3, 4, 5, 6
)
select * from final
order by total_minutes_streamed desc