select
    product_id,
    category,
    -- unit_price can drift slightly across orders in real systems (price
    -- changes over time); take the most recently observed price as the
    -- dimension's current value rather than an average, which would blur it.
    arg_max(unit_price, modified_at) as current_unit_price,
    count(distinct order_id) as times_ordered
from {{ ref('stg_orders') }}
group by 1, 2
