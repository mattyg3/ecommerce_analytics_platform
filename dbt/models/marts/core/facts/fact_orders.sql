{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge'
) }}

select
    order_id,
    user_id,
    session_id,
    order_ts,
    cast(order_ts as date) as order_date,
    order_status,
    item_count,
    order_total_amount
from {{ ref('stg_orders') }}

{% if is_incremental() %}
where order_ts > (
    select date_sub(max(order_ts), 1) 
    from {{ this }}
)
{% endif %}