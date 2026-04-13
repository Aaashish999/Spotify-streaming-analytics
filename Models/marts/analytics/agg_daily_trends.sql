with streams as (
    select * from {{ ref('fct_streams') }}
),
users as (
    select * from {{ ref('dim_users') }}
),
daily as (
    select
        s.stream_date,
        count(*)                                         as total_streams,
        count(distinct s.user_id)                        as daily_active_users,
        sum(s.duration_minutes)                          as total_minutes,
        sum(s.is_full_listen)                            as full_listens,
        round(100.0 * sum(s.is_full_listen) / count(*), 1) as completion_rate_pct,
        count(distinct case when u.subscription_type = 'premium'
            then s.user_id end)                          as premium_listeners,
        count(distinct case when u.subscription_type = 'free'
            then s.user_id end)                          as free_listeners
    from streams s
    left join users u on s.user_id = u.user_id
    group by 1
),
final as (
    select
        *,
        round(
            avg(total_streams) over (
                order by stream_date
                rows between 6 preceding and current row
            ), 1
        )                                                as streams_7day_avg,
        total_streams - lag(total_streams) over (
            order by stream_date
        )                                                as streams_vs_prev_day
    from daily
)
select * from final
order by stream_date