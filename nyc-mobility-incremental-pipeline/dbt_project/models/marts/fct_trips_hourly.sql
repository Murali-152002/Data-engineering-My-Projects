-- grain: pickup date x pickup hour x pickup zone
select
    pickup_date, pickup_hour, "PULocationID" as pu_location_id,
    count(*) as trips,
    sum(coalesce(passenger_count, 0)) as passengers,
    sum(trip_distance) as miles,
    sum(fare_amount) as fare_total,
    sum(tip_amount) as tip_total,
    sum(total_amount) as revenue,
    sum(trip_minutes) as trip_minutes_total,
    sum(case when coalesce(cbd_congestion_fee, 0) > 0 then 1 else 0 end) as cbd_fee_trips
from {{ ref('stg_trips') }}
group by 1, 2, 3
