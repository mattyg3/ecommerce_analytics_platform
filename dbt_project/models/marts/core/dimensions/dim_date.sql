{{ config(
    materialized='table'
) }}

with bounds as (
    select
        min(event_date) as min_date,
        max(event_date) as max_date
    from {{ ref('fact_events') }}
),

dates as (
    select
        explode(
            sequence(min_date, max_date, interval 1 day)
        ) as date
    from bounds
)

select
    date,
    day(date) as day,
    weekofyear(date) as week,
    month(date) as month,
    quarter(date) as quarter,
    year(date) as year,
    case when dayofweek(date) in (1, 7) then true else false end as is_weekend
from dates