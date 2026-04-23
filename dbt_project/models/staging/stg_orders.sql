{{ config(
    materialized = 'incremental',
    unique_key = 'order_id',
    incremental_strategy = 'delete+insert'
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

    {% if is_incremental() %}
        and pipeline_ingested_at >= (
            select coalesce(
                max(pipeline_ingested_at) - INTERVAL 1 DAY,
                TIMESTAMP '1900-01-01'
            ) --sliding window
            from {{ this }}
            )
    {% endif %}
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
  (
        select count(*)
        from (
            select unnest(items) as item
        ) t
    ) as item_count,
  (
        select sum(t.item.quantity * t.item.price)
        from (
            select unnest(items) as item
        ) t
    ) as order_total_amount,
  source_system,
  pipeline_ingested_at
from deduped