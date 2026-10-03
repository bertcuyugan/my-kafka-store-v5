-- One row per order (earliest order-placed event wins)
with source as (

    select * from {{ source('bronze', 'bronze_order_placed') }}

),

parsed as (

    select
        event_id,
        event_data:order_number::string     as order_number,
        event_data:order_id::int            as order_id,
        event_data:product_id::int          as product_id,
        event_data:customer_name::string    as customer_name,
        event_data:customer_email::string   as customer_email,
        event_data:item::string             as item,
        event_data:quantity::int            as quantity,
        event_data:total_price::double      as total_price,
        timestamp_millis(kafka_timestamp_ms) as ordered_at,
        _loaded_at
    from source

)

select *
from parsed
where order_number is not null
qualify row_number() over (partition by order_number order by ordered_at, event_id) = 1