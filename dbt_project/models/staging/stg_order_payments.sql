select
    order_id,
    payment_sequential,
    payment_type,
    cast(payment_installments as int) as payment_installments,
    cast(payment_value as double)     as payment_value
from {{ source('bronze', 'order_payments') }}
