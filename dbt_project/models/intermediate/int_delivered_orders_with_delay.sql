-- INTERMEDIATE MODEL — a reusable building block, not a business answer on
-- its own. This is dbt's own "intermediate" layer (staging -> intermediate
-- -> marts) — not to be confused with the bronze/silver/gold naming on the
-- Databricks/lakehouse side. Staging is roughly where "silver" lives;
-- "intermediate" is the layer medallion naming doesn't have a word for.
--
-- Both Q1 (gold_delivery_performance_by_state — everyone builds this) and
-- Q3 (gold_review_score_vs_delivery_delay — if you picked dbt) need the
-- exact same thing: delivered orders, with how many days late (or early)
-- each one arrived. Rather than compute that twice, it's computed ONCE
-- here, and both marts `ref()` this model instead of re-deriving it from
-- stg_orders themselves.
--
-- Required columns:
--   order_id
--   customer_id      -- so a downstream mart can still join to customers
--   delay_days        -- datediff(delivered_ts, estimated_ts); positive = late
--
-- Only orders with order_status = 'delivered' and a non-null
-- order_delivered_customer_ts belong here — anything still in transit or
-- cancelled has no delay to compute.

select
    o.order_id,
    o.customer_id,

    -- TODO: compute delay_days — number of days between
    -- o.order_delivered_customer_ts and o.order_estimated_delivery_ts.
    -- (positive = delivered later than estimated). Look at datediff().
    -- You'll only need to get this right ONCE — Q1 and Q3 (if you build
    -- it in dbt) both read it from here.
    null as delay_days

from {{ ref('stg_orders') }} o
where o.order_status = 'delivered'
  and o.order_delivered_customer_ts is not null
