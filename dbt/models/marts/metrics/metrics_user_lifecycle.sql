{{ config(
    materialized='table'
) }}

with sessions as (
    select
        user_id,
        min(session_start_ts) as first_session_ts,
        count(*) as total_sessions
    from {{ ref('fact_sessions') }}
    group by 1
),

orders as (
    select
        user_id,
        min(order_ts) as first_order_ts,
        count(distinct order_id) as total_orders,
        sum(order_total_amount) as lifetime_revenue,
        avg(order_total_amount) as avg_order_value
    from {{ ref('fact_orders') }}
    group by 1
)

select
    s.user_id,

    date(s.first_session_ts) as first_seen_date,
    date(o.first_order_ts) as first_order_date,

    -- datediff(
    --     date(o.first_order_ts),
    --     date(s.first_session_ts)
    -- ) as days_to_first_purchase,

    case
        when o.first_order_ts is null then null
        when o.first_order_ts < s.first_session_ts then null
        else datediff(
            date(o.first_order_ts),
            date(s.first_session_ts)
        )
        end as days_to_first_purchase,

    s.total_sessions,
    coalesce(o.total_orders, 0) as total_orders,
    coalesce(o.lifetime_revenue, 0) as lifetime_revenue,
    coalesce(o.avg_order_value, 0) as avg_order_value,

    case when o.total_orders > 1 then true else false end as is_repeat_buyer

from sessions s
left join orders o
  on s.user_id = o.user_id