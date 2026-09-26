{{ config(
    materialized='table'
) }}

with product_events as (

    select
        cast(e.event_ts as date) as date,
        e.product_id,

        count(distinct case
            when e.event_type = 'view_product'
            then e.session_id
        end) as product_views,

        count(distinct case
            when e.event_type = 'add_to_cart'
            then e.session_id
        end) as add_to_cart_sessions,

        count(distinct case
            when e.event_type = 'checkout_start'
            then e.session_id
        end) as checkout_sessions,

        count(distinct case
            when e.event_type = 'purchase'
            then e.session_id
        end) as purchase_sessions

    from {{ ref('fact_events') }} e

    where e.product_id is not null

    group by
        1,
        2
)

select
    date,
    product_id,
    product_views,
    add_to_cart_sessions,
    checkout_sessions,
    purchase_sessions

from product_events