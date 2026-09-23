-- QUESTION 1 (fixed, everyone builds this) — ELT version.
-- You already built the ETL version of this exact question in PySpark
-- (02_etl_style_transform.py). This is the same business question, done the
-- ELT way: the raw data is already loaded (stg_orders / stg_customers just
-- read your Bronze tables as-is) — ALL the transformation happens here,
-- after loading, in SQL.
--
-- Required output columns (so results are comparable across everyone,
-- and comparable to your PySpark version):
--   customer_state    -- 2-letter state code
--   order_count       -- count of delivered orders
--   avg_delay_days    -- avg(delivered_customer_ts - estimated_delivery_ts), in days
--   pct_late          -- % of orders where delay_days > 0
--
-- The "delivered, with delay_days" logic lives in
-- models/intermediate/int_delivered_orders_with_delay.sql — build that one
-- first (its own TODO is the datediff() itself). This mart just joins it to
-- customers for state and aggregates. If you also build Q3 in dbt, it reads
-- from that same intermediate model — you only compute delay_days once.

with delivered_orders as (

    select
        d.order_id,
        d.delay_days,
        c.customer_state

    from {{ ref('int_delivered_orders_with_delay') }} d
    join {{ ref('stg_customers') }} c on c.customer_id = d.customer_id

)

select
    customer_state,
    count(*) as order_count,

    -- TODO: avg delay in days across the group, rounded to 1 decimal
    null as avg_delay_days,

    -- TODO: % of orders in this state where delay_days > 0, rounded to 1 decimal
    null as pct_late

from delivered_orders
group by customer_state
order by pct_late desc
