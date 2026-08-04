select
    o.order_id,
    o.customer_id,
    o.product_id,
    cast(o.created_at as date) as order_date,
    o.quantity,
    o.unit_price,
    o.gross_amount,
    o.order_status,
    o.created_at,
    o.modified_at
from {{ ref('stg_orders') }} o
