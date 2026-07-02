-- Product dimension from the Open Food Facts API path, with per-100g
-- nutrition. Its natural key is the OFF barcode, so it stands apart from
-- the Instacart product catalogue.
select
    {{ dbt_utils.generate_surrogate_key(['product_code']) }} as food_product_key,
    product_code,
    product_name,
    brands,
    energy_kcal_100g,
    sugars_100g,
    fat_100g,
    proteins_100g,
    salt_100g,
    last_modified_at
from {{ ref('stg_off_products') }}
