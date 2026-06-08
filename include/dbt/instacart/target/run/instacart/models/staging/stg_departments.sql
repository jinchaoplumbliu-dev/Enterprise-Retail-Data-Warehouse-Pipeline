
  create view "instacart_warehouse"."staging"."stg_departments__dbt_tmp"
    
    
  as (
    select
    department_id,
    department as department_name
from "instacart_warehouse"."raw"."departments"
  );