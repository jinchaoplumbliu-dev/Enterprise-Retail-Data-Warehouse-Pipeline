
  create view "instacart_warehouse"."staging"."stg_orders__dbt_tmp"
    
    
  as (
    select
    order_id,
    user_id,
    eval_set,
    order_number,
    order_dow          as order_day_of_week,
    order_hour_of_day,
    days_since_prior_order
from "instacart_warehouse"."raw"."orders"
where eval_set in ('prior', 'train')
  );