-- Flatten the Open Food Facts VARIANT payload into typed columns and dedupe
-- by product code, keeping the most recently modified version.
with src as (
    select payload, last_modified_t
    from {{ source('raw', 'off_products_raw') }}
),

parsed as (
    select
        payload:code::string                          as product_code,
        payload:product_name::string                  as product_name,
        payload:brands::string                        as brands,
        payload:categories_tags                       as categories_tags,
        payload:nutriments:"energy-kcal_100g"::float  as energy_kcal_100g,
        payload:nutriments:sugars_100g::float         as sugars_100g,
        payload:nutriments:fat_100g::float            as fat_100g,
        payload:nutriments:proteins_100g::float       as proteins_100g,
        payload:nutriments:salt_100g::float           as salt_100g,
        last_modified_t,
        to_timestamp(last_modified_t)                 as last_modified_at
    from src
    where coalesce(payload:code::string, '') <> ''
)

select *
from parsed
qualify row_number() over (partition by product_code order by last_modified_t desc) = 1
