-- An order can't be delivered before it was shipped
select order_number, shipped_at, delivered_at
from {{ ref('fct_order_journey') }}
where delivered_at < shipped_at