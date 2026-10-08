-- Orders as they are in PostgreSQL (latest snapshot)
with latest as (

    {{ latest_snapshot(source('postgres', 'bronze_db_orders')) }}

)

select
    id                                as order_id,
    order_number,
    user_id,
    product_id,
    cast(quantity as int)             as quantity,
    total_price,
    status                            as db_status,
    cast(created_at as timestamp)     as created_at,
    cast(updated_at as timestamp)     as updated_at,
    cast(_extracted_at as timestamp)  as extracted_at
from latest