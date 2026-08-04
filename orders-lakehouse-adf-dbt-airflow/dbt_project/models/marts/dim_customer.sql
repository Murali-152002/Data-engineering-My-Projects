{#
  Customer dimension built from observed order activity (no separate
  customer-master source in this project - the mock API only exposes orders,
  matching the real "sometimes you only get transactional data" case). Each
  customer's first/last observed order date is a genuinely useful derived
  attribute for the fact table's downstream reporting, not just an ID list.
#}
select
    customer_id,
    min(created_at) as first_order_at,
    max(created_at) as last_order_at,
    count(*) as lifetime_order_count
from {{ ref('stg_orders') }}
group by 1
