select
    count(*)                                                     as total_orders,
    count(paid_at)                                               as paid,
    count(shipped_at)                                            as shipped,
    count(delivered_at)                                          as delivered,
    round(100.0 * count(delivered_at) / nullif(count(*), 0), 1)  as pct_delivered,
    round(avg(minutes_to_ship), 2)                               as avg_minutes_to_ship,
    round(avg(minutes_to_deliver), 2)                            as avg_minutes_to_deliver
from {{ ref('fct_order_journey') }}