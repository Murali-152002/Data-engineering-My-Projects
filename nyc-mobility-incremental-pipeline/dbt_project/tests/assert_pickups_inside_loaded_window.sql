-- silver should only contain pickups whose month matches the partition it was loaded from
select 1 from {{ ref('stg_trips') }} where strftime(pickup_date, '%Y-%m') != trip_month limit 1
