{{ config(
    materialized='table'
) }}

with order_items as (
    select
        date(o.order_ts) as date,
        oi.product_id,
        sum(oi.quantity) as units_sold,
        count(distinct o.order_id) as orders_with_product,
        sum(oi.quantity * oi.price) as product_revenue,
        avg(oi.price) as avg_price
    from {{ ref('fact_order_items') }} oi
    join {{ ref('fact_orders') }} o
      on oi.order_id = o.order_id
    group by 1, 2
),

product_sessions as (
    select
        date(e.event_ts) as date,
        e.product_id,
        count(distinct e.session_id) as sessions_viewed
    from {{ ref('fact_events') }} e
    where e.event_type = 'product_view'
    group by 1, 2
)

select
    oi.date,
    oi.product_id,

    oi.units_sold,
    oi.orders_with_product,
    oi.product_revenue,
    oi.avg_price,

    case
        when ps.sessions_viewed > 0
        then oi.orders_with_product / ps.sessions_viewed
        else 0
    end as product_conversion_rate

from order_items oi
left join product_sessions ps
  on oi.date = ps.date
 and oi.product_id = ps.product_id