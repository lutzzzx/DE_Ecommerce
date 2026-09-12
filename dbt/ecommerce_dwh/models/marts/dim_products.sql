select
    product_id,
    product_category
from {{ ref('stg_products') }}
