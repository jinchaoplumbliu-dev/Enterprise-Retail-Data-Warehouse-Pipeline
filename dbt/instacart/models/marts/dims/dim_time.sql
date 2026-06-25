-- dbt_utils.generate_series yields a `generated_number` column 1..N and is
-- cross-database (Postgres + Snowflake). It expands to a full `with ... select`,
-- so it must be a CTE *body* (not used after `from`); subtract 1 for 0-based.
with days as (
    {{ dbt_utils.generate_series(7) }}
),
hours as (
    {{ dbt_utils.generate_series(24) }}
),
spine as (
    select
        d.generated_number - 1 as order_day_of_week,
        h.generated_number - 1 as order_hour_of_day
    from days d
    cross join hours h
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