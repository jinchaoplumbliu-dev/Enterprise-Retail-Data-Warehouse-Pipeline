with day_of_week as (
    select generate_series(0, 6) as order_day_of_week
),
hour_of_day as (
    select generate_series(0, 23) as order_hour_of_day
),
spine as (
    select d.order_day_of_week, h.order_hour_of_day
    from day_of_week d
    cross join hour_of_day h
)

select
    {{ dbt_utils.generate_surrogate_key(['order_day_of_week', 'order_hour_of_day']) }} as time_key,
    order_day_of_week,
    -- ASSUMPTION: Instacart does not document the dow mapping; 0/1 are commonly
    -- taken as the weekend based on order-volume patterns. Treat as a guess.
    case order_day_of_week
        when 0 then 'Saturday' when 1 then 'Sunday'  when 2 then 'Monday'
        when 3 then 'Tuesday'  when 4 then 'Wednesday' when 5 then 'Thursday'
        when 6 then 'Friday'
    end as day_name,
    (order_day_of_week in (0, 1)) as is_weekend,
    order_hour_of_day,
    case
        when order_hour_of_day between 5 and 11  then 'Morning'
        when order_hour_of_day between 12 and 16 then 'Afternoon'
        when order_hour_of_day between 17 and 20 then 'Evening'
        else 'Night'
    end as day_part
from spine