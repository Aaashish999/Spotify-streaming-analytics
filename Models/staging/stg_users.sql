with source as (
    select * from {{ ref('raw_users') }}
),
cleaned as (
    select
        trim(user_id)                             as user_id,
        lower(trim(user_name))                    as user_name,
        upper(trim(country))                      as country,
        cast(age as integer)                      as age,
        lower(trim(subscription_type))            as subscription_type,
        cast(signup_date as date)                 as signup_date,
        datediff('day', cast(signup_date as date), current_date()) as days_since_signup
    from source
    where user_id is not null
)
select * from cleaned