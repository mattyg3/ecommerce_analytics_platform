{{ config(
    materialized='table',
    unique_key='user_id'
) }}

with user_events as (
    select
        user_id,
        min(event_ts) as first_seen_ts,
        max(event_ts) as last_seen_ts
    from {{ ref('fact_events') }}
    where user_id is not null
    group by user_id
),

user_orders as (
    select
        user_id,
        min(order_ts) as first_order_ts,
        count(distinct order_id) as total_orders,
        sum(order_total_amount) as lifetime_revenue
    from {{ ref('fact_orders') }}
    where user_id is not null
    group by user_id
)

select
    e.user_id,
    e.first_seen_ts,
    e.last_seen_ts,
    o.first_order_ts,
    coalesce(o.total_orders, 0) as total_orders,
    coalesce(o.lifetime_revenue, 0.0) as lifetime_revenue
from user_events e
left join user_orders o
    on e.user_id = o.user_id