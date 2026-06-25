{{
    config(
        materialized='incremental',
        unique_key=['order_id', 'product_id'],
        incremental_strategy='delete+insert',
        on_schema_change='fail'
    )
}}

with order_products as (
    select * from {{ ref('stg_order_products') }}
),
orders as (
    select * from {{ ref('stg_orders') }}
)

select
    op.order_id,
    op.product_id,
    op.order_number,
    op.add_to_cart_order,
    op.reordered,
    op.source,
    {{ dbt_utils.generate_surrogate_key(['op.product_id']) }} as product_key,
    {{ dbt_utils.generate_surrogate_key(['o.user_id']) }}     as user_key,
    {{ dbt_utils.generate_surrogate_key(['o.order_day_of_week', 'o.order_hour_of_day']) }} as time_key
from order_products op
inner join orders o on op.order_id = o.order_id

{% if is_incremental() %}
where op.order_number > (select max(order_number) from {{ this }})
{% endif %}