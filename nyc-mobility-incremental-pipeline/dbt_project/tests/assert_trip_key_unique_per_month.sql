select trip_month, trip_id, count(*) c from {{ ref('stg_trips') }} group by 1, 2 having count(*) > 1
