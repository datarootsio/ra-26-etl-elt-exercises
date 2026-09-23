select
    product_id,
    product_category_name
from {{ source('bronze', 'products') }}
