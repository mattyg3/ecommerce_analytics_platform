{{ config(
    materialized = 'incremental',
    unique_key = 'session_id',
    incremental_strategy = 'merge'
) }}

with events as (
  select
    session_id,
    user_id,
    event_ts
  from {{ ref('stg_clickstream_events') }}

  {% if is_incremental() %}
  where pipeline_ingested_at >
    (select max(pipeline_ingested_at) from {{ this }})
  {% endif %}
),

session_rollup as (
  select
    session_id,
    user_id,
    min(event_ts) as session_start_ts,
    max(event_ts) as session_end_ts,
    count(*)       as event_count
  from events
  group by session_id, user_id
)

select
  session_id,
  user_id,
  session_start_ts,
  session_end_ts,
  unix_timestamp(session_end_ts) - unix_timestamp(session_start_ts)
    as session_duration_sec,
  event_count
from session_rollup