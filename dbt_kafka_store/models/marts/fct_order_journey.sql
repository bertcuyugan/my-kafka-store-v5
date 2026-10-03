-- One row per order with its full lifecycle (same logic as gold_order_journey)
with orders as (
    select * from {{ ref('stg_orders') }}
),
payments as (
    select * from {{ ref('stg_payments') }}
),
shipments as (
    select * from {{ ref('stg_shipments') }}
),
deliveries as (
    select * from {{ ref('stg_deliveries') }}
)

select
    o.order_number,
    o.order_id,
    o.product_id,
    o.customer_name,
    o.customer_email,
    o.item,
    o.quantity,
    o.total_price,
    o.ordered_at,
    p.amount            as paid_amount,
    p.paid_at,
    s.tracking_number,
    s.shipped_at,
    d.delivered_at,
    case
        when d.order_number is not null then 'delivered'
        --when d.order_number is not null then 'done'
        when s.order_number is not null then 'shipped'
        when p.order_number is not null then 'paid'
        else 'placed'
    end                 as current_status,
    round((unix_timestamp(s.shipped_at)   - unix_timestamp(o.ordered_at)) / 60.0, 2) as minutes_to_ship,
    round((unix_timestamp(d.delivered_at) - unix_timestamp(o.ordered_at)) / 60.0, 2) as minutes_to_deliver
from orders o
left join payments   p on o.order_number = p.order_number
left join shipments  s on o.order_number = s.order_number
left join deliveries d on o.order_number = d.order_number