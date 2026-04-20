{{ config(
    materialized='table'
) }}

with session_events as (
    select
        date(e.event_ts) as date,
        e.session_id,

        max(case when e.event_type = 'product_view' then 1 else 0 end) as viewed_product,
        max(case when e.event_type = 'add_to_cart' then 1 else 0 end) as added_to_cart,
        max(case when e.event_type = 'checkout_start' then 1 else 0 end) as checkout_started
    from {{ ref('fact_events') }} e
    group by 1, 2
),

orders as (
    select
        date(order_ts) as date,
        session_id
    from {{ ref('fact_orders') }}
)

select
    se.date,

    count(distinct se.session_id) as sessions,
    sum(se.viewed_product) as sessions_with_product_view,
    sum(se.added_to_cart) as sessions_with_add_to_cart,
    sum(se.checkout_started) as sessions_with_checkout,
    count(distinct o.session_id) as sessions_with_order

from session_events se
left join orders o
  on se.session_id = o.session_id
 and se.date = o.date

group by 1