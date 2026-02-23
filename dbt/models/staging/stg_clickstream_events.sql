{{ config(
    materialized = 'incremental',
    unique_key = 'event_id',
    incremental_strategy = 'merge',
    on_schema_change='append_new_columns'
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

    {% if is_incremental() %}
        and pipeline_ingested_at >= (
            select coalesce(date_sub(max(pipeline_ingested_at), 1), timestamp('1900-01-01')) --sliding window
            from {{ this }}
            )
    {% endif %}
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