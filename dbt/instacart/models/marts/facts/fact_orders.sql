with orders as (
    select * from {{ ref('stg_orders') }}
),
order_metrics as (
    select * from {{ ref('int_order_metrics') }}
)

select
    o.order_id,                                                                 -- degenerate dimension
    {{ dbt_utils.generate_surrogate_key(['o.user_id']) }} as user_key,
    {{ dbt_utils.generate_surrogate_key(['o.order_day_of_week', 'o.order_hour_of_day']) }} as time_key,
    o.order_number,
    o.eval_set,
    o.days_since_prior_order,
    om.basket_size,
    om.num_reordered,
    om.reorder_ratio
from orders o
left join order_metrics om on o.order_id = om.order_id