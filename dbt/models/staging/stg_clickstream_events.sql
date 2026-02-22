{{ config(
    materialized = 'incremental',
    unique_key = 'event_id',
    incremental_strategy = 'merge'
) }}

with ranked_events as (
    select
        *,
        row_number() over (
            partition by event_id
            order by pipeline_ingested_at desc
        ) as rn
    from {{ source('bronze', 'clickstream') }}
    where event_time is not null
),

deduped as (
    select *
    from ranked_events
    where rn = 1
)

select
  event_id,
  event_type,
  user_id,
  session_id,
  product_id,
  cast(event_time as timestamp) as event_ts,
  date(event_time) as event_date,
  upper(country) as country,
  device,
  experiment_id,
  referrer,
  source_system,
  pipeline_ingested_at
from deduped

{% if is_incremental() %}
and pipeline_ingested_at >
    (select max(pipeline_ingested_at) from {{ this }})
{% endif %}