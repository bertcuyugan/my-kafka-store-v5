-- One row per product: catalogue data from PostgreSQL + sales from Kafka events
with products as (

    select * from {{ ref('stg_db__products') }}

),

sales as (

    select
        product_id,
        count(*)                     as times_ordered,
        sum(quantity)                as units_sold,
        round(sum(total_price), 2)   as revenue
    from {{ ref('stg_orders') }}
    group by product_id

)

select
    p.product_id,
    p.product_name,
    p.category,
    p.price,
    p.stock,
    round(p.price * p.stock, 2)        as stock_value,
    coalesce(s.times_ordered, 0)       as times_ordered,
    coalesce(s.units_sold, 0)          as units_sold,
    coalesce(s.revenue, 0)             as revenue,
    p.created_at
from products p
left join sales s
    on p.product_id = s.product_id