with products as (
    select * from {{ ref('stg_products') }}
),
aisles as (
    select * from {{ ref('stg_aisles') }}
),
departments as (
    select * from {{ ref('stg_departments') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['p.product_id']) }} as product_key,
    p.product_id,
    p.product_name,
    p.aisle_id,
    a.aisle_name,
    p.department_id,
    d.department_name
from products p
left join aisles a       on p.aisle_id = a.aisle_id
left join departments d  on p.department_id = d.department_id