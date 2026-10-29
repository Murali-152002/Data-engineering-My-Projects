select
    weather_date as date_key,
    dayofweek(weather_date) as day_of_week,          -- 0 = Sunday
    dayofweek(weather_date) in (0, 6) as is_weekend,
    tmax_f, tmin_f, prcp_in, snow_in, avg_wind_mph,
    case when snow_in > 0 then 'snow'
         when prcp_in >= 0.10 then 'rain'
         when prcp_in > 0 then 'drizzle'
         else 'dry' end as weather_band,
    case when tmax_f < 40 then 'cold (<40F)'
         when tmax_f < 70 then 'mild (40-69F)'
         else 'warm (70F+)' end as temp_band
from {{ ref('stg_weather') }}
