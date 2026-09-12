select
    product_id,
    coalesce(product_category_name_english, 'unknown') as product_category
from {{ source('staging', 'products') }}
