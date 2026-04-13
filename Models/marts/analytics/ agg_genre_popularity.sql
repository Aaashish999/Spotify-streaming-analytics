with streams as (
    select * from {{ ref('fct_streams') }}
),
final as (
    select
        stream_month,
        genre,
        count(*)                                         as total_streams,
        count(distinct user_id)                          as unique_listeners,
        sum(duration_minutes)                            as total_minutes,
        round(avg(duration_minutes), 2)                  as avg_duration_minutes,
        round(
            100.0 * count(*) / sum(count(*)) over (partition by stream_month),
            2
        )                                                as genre_share_pct
    from streams
    group by 1, 2
)
select * from final
order by stream_month, total_streams desc