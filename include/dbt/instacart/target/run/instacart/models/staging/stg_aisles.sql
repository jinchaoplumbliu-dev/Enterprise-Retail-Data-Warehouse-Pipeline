
  create view "instacart_warehouse"."staging"."stg_aisles__dbt_tmp"
    
    
  as (
    select
    aisle_id,
    aisle as aisle_name
from "instacart_warehouse"."raw"."aisles"
  );