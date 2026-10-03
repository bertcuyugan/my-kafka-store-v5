select
    customer_email,
    max(customer_name)           as customer_name,
    count(*)                     as total_orders,
    round(sum(total_price), 2)   as total_spent,
    round(avg(total_price), 2)   as avg_order_value,
    min(ordered_at)              as first_order_at,
    max(ordered_at)              as last_order_at
from {{ ref('stg_orders') }}
group by customer_email