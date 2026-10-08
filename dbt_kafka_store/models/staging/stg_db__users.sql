-- Customers as they are in PostgreSQL (latest snapshot)
with latest as (

    {{ latest_snapshot(source('postgres', 'bronze_db_users')) }}

)

select
    id                                as user_id,
    name                              as customer_name,
    email                             as customer_email,
    cast(created_at as timestamp)     as registered_at,
    cast(_extracted_at as timestamp)  as extracted_at
from latest