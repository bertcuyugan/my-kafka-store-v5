-- Products as they are in PostgreSQL (latest snapshot)
with latest as (

    {{ latest_snapshot(source('postgres', 'bronze_db_products')) }}

)

select
    id                                as product_id,
    name                              as product_name,
    description,
    category,
    price,
    cast(stock as int)                as stock,
    image_url,
    cast(created_at as timestamp)     as created_at,
    cast(_extracted_at as timestamp)  as extracted_at
from latest