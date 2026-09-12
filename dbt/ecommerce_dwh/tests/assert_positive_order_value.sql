select order_id, gross_merchandise_value
from {{ ref('fact_orders') }}
where gross_merchandise_value < 0
