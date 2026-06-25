{{ config(materialized='table') }}
-- This model aggregates 33M order-item records and is reused by two downstream models
-- so it is materialised as a table to avoid repeated computation.

select
    order_id,
    count(*)                            as basket_size,
    sum(reordered)                      as num_reordered,
    round(avg(reordered::numeric), 4)   as reorder_ratio
from {{ ref('stg_order_products') }}
group by order_id