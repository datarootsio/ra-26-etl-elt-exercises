-- Staging = light cleanup only (rename, cast, dedupe). No business logic here.
select
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    upper(customer_state) as customer_state
from {{ source('bronze', 'customers') }}
