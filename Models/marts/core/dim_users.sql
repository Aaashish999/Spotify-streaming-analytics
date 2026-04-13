with users as (
    select * from {{ ref('stg_users') }}
),
final as (
    select
        user_id,
        user_name,
        country,
        age,
        subscription_type,
        signup_date,
        days_since_signup,
        case
            when age between 18 and 24 then '18-24'
            when age between 25 and 34 then '25-34'
            when age between 35 and 44 then '35-44'
            else '45+'
        end as age_bucket,
        case
            when days_since_signup < 90  then 'new'
            when days_since_signup < 365 then 'growing'
            else 'established'
        end as user_tenure
    from users
)
select * from final
