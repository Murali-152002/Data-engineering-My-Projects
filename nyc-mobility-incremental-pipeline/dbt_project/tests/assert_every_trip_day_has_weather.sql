select distinct f.pickup_date from {{ ref('fct_trips_hourly') }} f
left join {{ ref('dim_date') }} d on d.date_key = f.pickup_date where d.date_key is null
