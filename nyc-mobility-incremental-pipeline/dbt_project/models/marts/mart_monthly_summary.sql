-- includes the Manhattan congestion-pricing fee (cbd_congestion_fee), which exists in the 2025 files
select strftime(pickup_date, '%Y-%m') as month,
       sum(trips) as trips, round(sum(revenue), 0) as revenue,
       round(sum(revenue) / sum(trips), 2) as revenue_per_trip,
       sum(cbd_fee_trips) as cbd_fee_trips,
       round(100.0 * sum(cbd_fee_trips) / sum(trips), 1) as cbd_fee_trip_pct
from {{ ref('fct_trips_hourly') }} group by 1
