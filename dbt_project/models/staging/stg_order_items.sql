{{ config(
    materialized = 'incremental',
    unique_key = ['order_id', 'product_id'],
    incremental_strategy = 'merge'
) }}

select
  o.order_id,
  o.user_id,
  o.session_id,
  item.product_id,
  item.quantity,
  item.price,
  item.quantity * item.price as line_amount,
  o.order_ts,
  o.pipeline_ingested_at
from {{ ref('stg_orders') }} o
lateral view explode(o.items) as item

{% if is_incremental() %}
where pipeline_ingested_at >= (
    select coalesce(date_sub(max(pipeline_ingested_at), 1), timestamp('1900-01-01')) --sliding window
    from {{ this }}
    )
{% endif %}