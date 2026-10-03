{{
    config(
        materialized='incremental',
        unique_key='event_id',
        incremental_strategy='merge'
    )
}}

with events as (

    select event_id, 'order-placed'     as event_type, event_data:order_number::string as order_number,
           timestamp_millis(kafka_timestamp_ms) as event_time, _loaded_at
    from {{ source('bronze', 'bronze_order_placed') }}

    union all

    select event_id, 'payment-received' as event_type, event_data:order_number::string as order_number,
           timestamp_millis(kafka_timestamp_ms) as event_time, _loaded_at
    from {{ source('bronze', 'bronze_payment_received') }}

    union all

    select event_id, 'item-shipped'     as event_type, event_data:order_number::string as order_number,
           timestamp_millis(kafka_timestamp_ms) as event_time, _loaded_at
    from {{ source('bronze', 'bronze_item_shipped') }}

    union all

    select event_id, 'item-delivered'   as event_type, event_data:order_number::string as order_number,
           timestamp_millis(kafka_timestamp_ms) as event_time, _loaded_at
    from {{ source('bronze', 'bronze_item_delivered') }}

)

select *
from events

{% if is_incremental() %}
where _loaded_at > (select max(_loaded_at) from {{ this }})
{% endif %}

qualify row_number() over (partition by event_id order by _loaded_at) = 1