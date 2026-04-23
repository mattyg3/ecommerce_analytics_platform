{{ config(
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='delete+insert',
    partition_by=['event_date']
) }}

select
    event_id,
    event_ts,
    event_date,
    event_type,
    user_id,
    session_id,
    product_id,
    country,
    device,
    referrer,
    source_system,
    experiment_id
from {{ ref('stg_clickstream_events') }}

{% if is_incremental() %}
where event_ts >= (
    select max(event_ts) - INTERVAL 1 DAY
    from {{ this }}
)
{% endif %}