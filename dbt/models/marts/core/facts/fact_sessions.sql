{{ config(
    materialized='incremental',
    unique_key='session_id',
    incremental_strategy='merge'
) }}

with sessions as (
    select
        session_id,
        user_id,
        session_start_ts,
        session_end_ts,
        session_duration_sec,
        event_count
    from {{ ref('stg_clickstream_sessions') }}

    {% if is_incremental() %}
    where session_start_ts >= (
        select date_sub(max(session_start_ts), 1) 
        from {{ this }}
    )
    {% endif %}
),

orders_by_session as (
    select
        session_id,
        count(distinct order_id) as orders_in_session,
        sum(order_total_amount) as revenue_in_session
    from {{ ref('stg_orders') }}
    group by session_id
)

select
    s.session_id,
    s.user_id,
    s.session_start_ts,
    s.session_end_ts,
    s.session_duration_sec,
    s.event_count,

    coalesce(o.orders_in_session, 0) > 0 as has_order,
    coalesce(o.orders_in_session, 0) as orders_in_session,
    coalesce(o.revenue_in_session, 0.0) as revenue_in_session

from sessions s
left join orders_by_session o
    on s.session_id = o.session_id