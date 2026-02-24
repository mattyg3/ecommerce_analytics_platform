{{ config(
    materialized='table',
    unique_key='product_id'
) }}

select
    product_id,
    min(order_ts) as first_sold_ts,
    sum(quantity) as total_units_sold,
    sum(line_amount) as total_revenue
from {{ ref('fact_order_items') }}
where product_id is not null
group by product_id