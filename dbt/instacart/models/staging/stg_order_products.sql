with prior as (
    select
        order_id, product_id, add_to_cart_order, reordered, order_number,
        'prior' as source
    from {{ source('raw', 'order_products_prior') }}
),

train as (
    select
        order_id, product_id, add_to_cart_order, reordered, order_number,
        'train' as source
    from {{ source('raw', 'order_products_train') }}
)

select * from prior
union all
select * from train