select
    {{ dbt_utils.generate_surrogate_key(['user_id']) }} as user_key,
    user_id,
    total_orders,
    avg_days_between_orders,
    total_items_purchased,
    avg_basket_size,
    reorder_ratio
from {{ ref('int_user_metrics') }}