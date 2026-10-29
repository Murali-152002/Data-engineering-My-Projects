select
    z.zone, z.borough, z.is_airport,
    sum(f.trips) as trips, round(sum(f.revenue), 0) as revenue,
    round(sum(f.revenue) / sum(f.trips), 2) as revenue_per_trip,
    round(100.0 * sum(f.tip_total) / nullif(sum(f.fare_total), 0), 2) as tip_pct_of_fare,
    rank() over (order by sum(f.trips) desc) as trips_rank
from {{ ref('fct_trips_hourly') }} f join {{ ref('dim_zone') }} z on z.location_id = f.pu_location_id
group by 1, 2, 3
