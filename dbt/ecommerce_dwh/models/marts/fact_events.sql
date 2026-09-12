select
    event_id,
    event_type,
    event_timestamp,
    event_timestamp::date as event_date,
    customer_id,
    product_id,
    session_id,
    price,
    device
from {{ source('staging', 'events') }}
