-- QUESTION 3 — build this file ONLY if you chose dbt/ELT for this question.
-- (If you chose PySpark instead, do this in a notebook and delete/ignore this file.)
--
-- Business question: does shipping speed drive satisfaction?
--
-- Required output columns:
--   delay_bucket      -- one of: 'early_or_on_time', '1_3_days_late',
--                         '4_7_days_late', '8_plus_days_late'
--   order_count
--   avg_review_score  -- rounded to 2 decimals

-- Reuses models/intermediate/int_delivered_orders_with_delay.sql — you
-- already built the "delivered orders + delay_days" logic once for Q1, so
-- this mart just buckets that same delay_days and joins to reviews. If
-- your Q1 build isn't finished yet, do that first.

with bucketed as (

    select
        d.order_id,
        r.review_score,

        -- TODO: turn delay_days into the 4 buckets described above using a
        -- case/when. early_or_on_time = delay_days <= 0.
        null as delay_bucket

    from {{ ref('int_delivered_orders_with_delay') }} d
    join {{ ref('stg_order_reviews') }} r on r.order_id = d.order_id

)

select
    delay_bucket,
    count(*) as order_count,
    -- TODO: avg(review_score), rounded to 2 decimals
    null as avg_review_score

from bucketed
group by delay_bucket
