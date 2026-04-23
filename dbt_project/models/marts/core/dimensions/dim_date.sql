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
        unnest(
            generate_series(min_date, max_date, interval 1 day)
        ) as date
    from bounds
)

select
    date,
    extract(day from date) as day,
    extract(week from date) as week,
    extract(month from date) as month,
    extract(quarter from date) as quarter,
    extract(year from date) as year,
    case when extract(dow from date) in (0, 6) then true else false end as is_weekend
from dates