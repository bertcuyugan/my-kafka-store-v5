select
    item,
    count(*)                     as times_ordered,
    sum(quantity)                as units_sold,
    round(sum(total_price), 2)   as revenue
from {{ ref('stg_orders') }}
group by item