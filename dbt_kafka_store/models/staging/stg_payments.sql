-- One row per paid order
with source as (

    select * from {{ source('bronze', 'bronze_payment_received') }}

),

parsed as (

    select
        event_id,
        event_data:order_number::string     as order_number,
        event_data:payment_id::int          as payment_id,
        event_data:amount::double           as amount,
        timestamp_millis(kafka_timestamp_ms) as paid_at,
        _loaded_at
    from source

)

select *
from parsed
where order_number is not null
qualify row_number() over (partition by order_number order by paid_at, event_id) = 1