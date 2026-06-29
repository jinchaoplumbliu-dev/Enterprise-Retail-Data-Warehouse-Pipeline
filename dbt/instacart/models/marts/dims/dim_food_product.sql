-- A product dimension sourced from the Open Food Facts API (the incremental
-- ingestion path), with per-100g nutrition attributes. A standalone data
-- product: its natural key is the OFF barcode, independent of the Instacart
-- catalogue.
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
