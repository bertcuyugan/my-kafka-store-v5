-- Shipments as they are in PostgreSQL (latest snapshot)
with latest as (

    {{ latest_snapshot(source('postgres', 'bronze_db_shipments')) }}

)

select
    id                                as shipment_id,
    order_id,
    tracking_number,
    status                            as shipment_status,
    cast(shipped_at as timestamp)     as shipped_at,
    cast(delivered_at as timestamp)   as delivered_at,
    cast(created_at as timestamp)     as created_at,
    cast(_extracted_at as timestamp)  as extracted_at
from latest