{{ config(
    materialized='table'
) }}

with sessions as (
    select
        date(session_start_ts) as date,
        count(*) as sessions,
        count(distinct user_id) as daily_active_users
    from {{ ref('fact_sessions') }}
    group by 1
),

orders as (
    select
        date(order_ts) as date,
        count(distinct order_id) as orders,
        sum(order_total_amount) as revenue,
        avg(order_total_amount) as avg_order_value
    from {{ ref('fact_orders') }}
    group by 1
),

sessions_with_orders as (
    select
        date(s.session_start_ts) as date,
        count(distinct s.session_id) as sessions_with_orders
    from {{ ref('fact_sessions') }} s
    join {{ ref('fact_orders') }} o
      on s.session_id = o.session_id
    group by 1
)

select
    d.date,

    coalesce(s.daily_active_users, 0) as daily_active_users,
    coalesce(s.sessions, 0) as sessions,
    coalesce(o.orders, 0) as orders,
    coalesce(o.revenue, 0) as revenue,
    coalesce(o.avg_order_value, 0) as avg_order_value,

    case
        when s.sessions > 0 then o.orders / s.sessions
        else 0
    end as conversion_rate,

    case
        when s.sessions > 0 then swo.sessions_with_orders / s.sessions
        else 0
    end as sessions_with_order_pct

from {{ ref('dim_date') }} d
left join sessions s on d.date = s.date
left join orders o on d.date = o.date
left join sessions_with_orders swo on d.date = swo.date