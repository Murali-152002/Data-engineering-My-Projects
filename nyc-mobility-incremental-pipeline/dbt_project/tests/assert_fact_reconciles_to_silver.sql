-- every silver trip must be counted exactly once in the fact table
select 1 from (select (select sum(trips) from {{ ref('fct_trips_hourly') }}) as f,
                      (select count(*) from {{ ref('stg_trips') }}) as s) where f != s
