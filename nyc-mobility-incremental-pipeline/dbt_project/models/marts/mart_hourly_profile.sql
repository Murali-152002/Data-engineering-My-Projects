select d.is_weekend, f.pickup_hour,
       round(sum(f.trips) * 1.0 / count(distinct f.pickup_date), 0) as avg_trips_per_day,
       round(sum(f.revenue) / sum(f.trips), 2) as avg_revenue_per_trip
from {{ ref('fct_trips_hourly') }} f join {{ ref('dim_date') }} d on d.date_key = f.pickup_date
group by 1, 2
