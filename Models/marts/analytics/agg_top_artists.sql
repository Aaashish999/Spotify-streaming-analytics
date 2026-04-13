with streams as (
    select * from {{ ref('fct_streams') }}
),
artists as (
    select * from {{ ref('dim_artists') }}
),
final as (
    select
        a.artist_id,
        a.artist_name,
        a.genre,
        a.artist_tier,
        count(*)                                         as total_streams,
        sum(s.is_full_listen)                            as full_listens,
        sum(s.duration_minutes)                          as total_minutes_streamed,
        round(avg(s.duration_minutes), 2)                as avg_minutes_per_stream,
        count(distinct s.user_id)                        as unique_listeners,
        round(
            100.0 * sum(case when s.was_skipped then 1 else 0 end) / count(*),
            1
        )                                                as skip_rate_pct,
        row_number() over (order by count(*) desc)       as stream_rank
    from streams s
    left join artists a on s.artist_id = a.artist_id
    group by 1, 2, 3, 4
)
select * from final
order by stream_rank