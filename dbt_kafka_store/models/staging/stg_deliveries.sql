-- One row per delivered order
with source as (

    select * from {{ source('bronze', 'bronze_item_delivered') }}

),

parsed as (

    select
        event_id,
        event_data:order_number::string     as order_number,
        event_data:tracking_number::string  as tracking_number,
        timestamp_millis(kafka_timestamp_ms) as delivered_at,
        _loaded_at
    from source

)

select *
from parsed
where order_number is not null
qualify row_number() over (partition by order_number order by delivered_at, event_id) = 1