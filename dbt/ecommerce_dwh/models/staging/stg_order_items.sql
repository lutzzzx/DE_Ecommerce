with source as (
    select * from {{ source('staging', 'order_items') }}
)
select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    price,
    freight_value,
    price + freight_value as item_total_value
from source
where price >= 0
