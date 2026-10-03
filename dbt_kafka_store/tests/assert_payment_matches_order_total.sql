-- The amount Stripe charged must equal the order total (within 1 cent)
select
    o.order_number,
    o.total_price,
    p.amount
from {{ ref('stg_orders') }} o
join {{ ref('stg_payments') }} p
    on o.order_number = p.order_number
where abs(o.total_price - p.amount) > 0.01