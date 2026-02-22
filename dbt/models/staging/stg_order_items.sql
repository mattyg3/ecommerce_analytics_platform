{{ config(
    materialized = 'incremental',
    unique_key = 'order_id',
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
and pipeline_ingested_at >
    (select max(pipeline_ingested_at) from {{ this }})
{% endif %}