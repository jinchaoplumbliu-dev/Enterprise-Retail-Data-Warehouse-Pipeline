with orders as (
    select * from {{ ref('stg_orders') }}
),

order_metrics as (
    select * from {{ ref('int_order_metrics') }}
)

select
    o.user_id,
    count(distinct o.order_id)                                          as total_orders,
    round(avg(o.days_since_prior_order), 2)                            as avg_days_between_orders,
    sum(om.basket_size)                                                as total_items_purchased,
    round(avg(om.basket_size), 2)                                      as avg_basket_size,
    round(sum(om.num_reordered)::numeric / nullif(sum(om.basket_size), 0), 4) as reorder_ratio
from orders o
left join order_metrics om on o.order_id = om.order_id
group by o.user_id