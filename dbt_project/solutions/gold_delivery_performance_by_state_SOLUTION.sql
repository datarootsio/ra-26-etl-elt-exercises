-- QUESTION 1 (fixed, everyone builds this) — ELT version.
-- You already built the ETL version of this exact question in PySpark
-- (02_etl_style_transform.py). This is the same business question, done the
-- ELT way: raw data is already loaded (via stg_orders / stg_customers,
-- which just read your Bronze tables) — all the transformation happens here,
-- after loading, in SQL.
--
-- Required output columns (so results are comparable across everyone,
-- and comparable to your PySpark version):
--   customer_state          -- 2-letter state code
--   order_count              -- count of delivered orders
--   avg_delay_days           -- avg(delivered_customer_ts - estimated_delivery_ts), in days
--   pct_late                 -- % of orders where delay_days > 0
--
-- Delay_days is computed once in models/intermediate/int_delivered_orders_with_delay.sql
-- and reused here and in gold_review_score_vs_delivery_delay — no need to
-- re-derive it from stg_orders in every mart that needs it.

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
    count(*)                                            as order_count,
    round(avg(delay_days), 1)                            as avg_delay_days,
    round(100.0 * sum(case when delay_days > 0 then 1 else 0 end) / count(*), 1) as pct_late

from delivered_orders
group by customer_state
order by pct_late desc
