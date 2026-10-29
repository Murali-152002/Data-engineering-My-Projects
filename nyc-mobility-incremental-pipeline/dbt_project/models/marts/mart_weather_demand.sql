-- Does weather change demand and tipping? Split by weekend/weekday so day-of-week doesn't confound.
with daily as (
    select pickup_date, sum(trips) as trips, sum(revenue) as revenue, sum(fare_total) as fare_total,
           sum(tip_total) as tip_total, sum(trip_minutes_total) as minutes
    from {{ ref('fct_trips_hourly') }} group by 1
)
select
    d.is_weekend, d.weather_band,
    count(*) as days,
    round(avg(daily.trips), 0) as avg_trips_per_day,
    round(sum(daily.revenue) / sum(daily.trips), 2) as avg_revenue_per_trip,
    round(100.0 * sum(daily.tip_total) / sum(daily.fare_total), 2) as tip_pct_of_fare,
    round(sum(daily.minutes) / sum(daily.trips), 2) as avg_trip_minutes
from daily join {{ ref('dim_date') }} d on d.date_key = daily.pickup_date
group by 1, 2
