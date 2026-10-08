-- Payments as they are in PostgreSQL (latest snapshot)
with latest as (

    {{ latest_snapshot(source('postgres', 'bronze_db_payments')) }}

)

select
    id                                as payment_id,
    order_id,
    amount,
    status                            as payment_status,
    stripe_payment_id,
    cast(created_at as timestamp)     as paid_at,
    cast(_extracted_at as timestamp)  as extracted_at
from latest