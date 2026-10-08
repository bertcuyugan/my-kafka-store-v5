-- Do PostgreSQL (source of truth) and the Kafka events agree on every order?
with db_orders as (

    select order_number, db_status, total_price as db_total_price
    from {{ ref('stg_db__orders') }}
    where db_status <> 'pending'      -- pending = checkout never completed, so no events exist by design

),

event_orders as (

    select order_number, current_status as event_status, total_price as event_total_price
    from {{ ref('fct_order_journey') }}

)

select
    coalesce(d.order_number, e.order_number)  as order_number,
    d.db_status,
    e.event_status,
    d.db_total_price,
    e.event_total_price,
    case
        when e.order_number is null                              then 'missing_in_events'
        when d.order_number is null                              then 'missing_in_db'
        when d.db_status <> e.event_status                       then 'status_mismatch'
        when abs(d.db_total_price - e.event_total_price) > 0.01  then 'amount_mismatch'
        else 'ok'
    end                                         as reconciliation_status
from db_orders d
full outer join event_orders e
    on d.order_number = e.order_number
