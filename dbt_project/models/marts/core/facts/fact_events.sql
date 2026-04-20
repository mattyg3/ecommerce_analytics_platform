{{ config(
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='merge',
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
    select date_sub(max(event_ts),1) 
    from {{ this }}
)
{% endif %}