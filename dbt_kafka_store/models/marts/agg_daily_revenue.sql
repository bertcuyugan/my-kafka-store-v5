select
    date(ordered_at)             as order_date,
    count(*)                     as orders,
    sum(quantity)                as units_sold,
    round(sum(total_price), 2)   as revenue,
    round(avg(total_price), 2)   as avg_order_value
from {{ ref('stg_orders') }}
group by date(ordered_at)