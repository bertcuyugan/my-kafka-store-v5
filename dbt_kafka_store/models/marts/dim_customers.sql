select
    customer_email,
    max(customer_name)           as customer_name,
    count(*)                     as total_orders,
    round(sum(total_price), 2)   as total_spent,
    round(avg(total_price), 2)   as avg_order_value,
    round(max(total_price), 2)   as max_order_value, -- new entry CI project
    --cast(null as double)         as max_order_value, -- CI fail on purpose
    min(ordered_at)              as first_order_at,
    max(ordered_at)              as last_order_at
from {{ ref('stg_orders') }}
group by customer_email