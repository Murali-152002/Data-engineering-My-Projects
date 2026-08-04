{#
  Simple generated date spine covering the observed order range - a real
  dim_date would extend further in both directions, but this stays scoped to
  what the data actually needs for a personal project.
#}
with bounds as (
    select
        cast(min(created_at) as date) as min_date,
        cast(max(created_at) as date) as max_date
    from {{ ref('stg_orders') }}
),
spine as (
    select unnest(generate_series(
        (select min_date from bounds),
        (select max_date from bounds),
        interval 1 day
    )) as order_date
)
select
    order_date,
    extract(year from order_date) as year,
    extract(month from order_date) as month,
    extract(dow from order_date) as day_of_week,
    case when extract(dow from order_date) in (0, 6) then true else false end as is_weekend
from spine
