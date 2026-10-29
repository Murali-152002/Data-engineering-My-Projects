select pickup_date, pickup_hour, pu_location_id, count(*) c
from {{ ref('fct_trips_hourly') }} group by 1, 2, 3 having count(*) > 1
