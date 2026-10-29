select 1 from {{ ref('stg_trips') }} where total_amount < 0 limit 1
