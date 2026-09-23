-- INTERMEDIATE MODEL SOLUTION — copy this over
-- models/intermediate/int_delivered_orders_with_delay.sql.
--
-- Both gold marts below read from this one model instead of each
-- re-deriving delay_days from stg_orders independently.

select
    o.order_id,
    o.customer_id,
    datediff(o.order_delivered_customer_ts, o.order_estimated_delivery_ts) as delay_days

from {{ ref('stg_orders') }} o
where o.order_status = 'delivered'
  and o.order_delivered_customer_ts is not null
