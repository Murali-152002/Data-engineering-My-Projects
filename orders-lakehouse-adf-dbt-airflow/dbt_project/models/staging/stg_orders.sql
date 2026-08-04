{#
  Staging: clean + type Bronze's raw (string-typed) order rows.
  Bronze is append-only and can contain the same order_id more than once
  across incremental pulls (e.g. a status change caught by the modified_at
  watermark re-lands that order). Dedup here keeps the latest version per
  order_id - this is the dbt/SQL equivalent of the MERGE-based upsert used
  in the other two projects, just expressed as a window function instead of
  a MERGE statement since dbt models are pure SELECTs, not imperative merges.
#}

with source as (
    select * from {{ source('bronze', 'orders') }}
),

typed as (
    select
        order_id,
        nullif(customer_id, '') as customer_id,
        product_id,
        category,
        -- mirrors the "3.0"-style float-string quantity quirk handled in
        -- the other two projects' Silver layers - cast via double first so
        -- both "2" and "2.0" coerce cleanly instead of erroring.
        cast(cast(quantity as double) as integer) as quantity,
        cast(unit_price as double) as unit_price,
        order_status,
        cast(created_at as timestamp) as created_at,
        cast(modified_at as timestamp) as modified_at,
        _source_file,
        cast(_ingested_at as timestamp) as _ingested_at
    from source
),

deduped as (
    select *,
        row_number() over (
            partition by order_id
            order by modified_at desc, _ingested_at desc
        ) as _rn
    from typed
)

select
    order_id,
    customer_id,
    product_id,
    category,
    quantity,
    unit_price,
    round(quantity * unit_price, 2) as gross_amount,
    order_status,
    created_at,
    modified_at
from deduped
where _rn = 1
  and customer_id is not null   -- can't be joined to dim_customer without one; same rule as the other projects' Silver layer
