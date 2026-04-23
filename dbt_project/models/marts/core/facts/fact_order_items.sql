{{ config(
    materialized='incremental',
    unique_key=['order_id', 'product_id'],
    incremental_strategy='delete+insert'
) }}

select
    order_id,
    product_id,
    user_id,
    session_id,
    quantity,
    price,
    line_amount,
    order_ts,
    cast(order_ts as date) as order_date
from {{ ref('stg_order_items') }}

{% if is_incremental() %}
where order_ts >= (
    select max(order_ts) - INTERVAL 1 DAY
    from {{ this }}
)
{% endif %}