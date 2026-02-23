{{ config(
    materialized = 'incremental',
    unique_key = 'order_id',
    incremental_strategy = 'merge'
) }}

with ranked_orders as (
    select
        *,
        row_number() over (
            partition by order_id
            order by pipeline_ingested_at desc
        ) as rn
    from {{ source('bronze', 'orders') }}
    where order_time is not null
),

deduped as (
    select *
    from ranked_orders
    where rn = 1
)

select
  order_id,
  user_id,
  session_id,
  items,
  order_status,
  cast(order_time as timestamp) as order_ts,
  size(items) as item_count,
  aggregate(
    items,
    0D,
    (acc, x) -> acc + (x.quantity * x.price)
  ) as order_total_amount,
  source_system,
  pipeline_ingested_at
from deduped
{% if is_incremental() %}
where pipeline_ingested_at >
    (select max(pipeline_ingested_at) from {{ this }})
{% endif %}