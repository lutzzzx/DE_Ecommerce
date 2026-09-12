with source as (
    select * from {{ source('staging', 'orders') }}
)
select
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    case
        when order_delivered_customer_date is not null
        then extract(day from order_delivered_customer_date - order_purchase_timestamp)
    end as delivery_days
from source
where order_id is not null
