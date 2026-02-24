{{ config(
    materialized='table',
    unique_key='session_id'
) }}

with ranked_events as (
    select
        session_id,
        country,
        device,
        referrer,
        source_system,
        row_number() over (
            partition by session_id
            order by event_ts asc
        ) as rn
    from {{ ref('fact_events') }}
    where session_id is not null
)

select
    session_id,
    country,
    device,
    referrer,
    source_system
from ranked_events
where rn = 1