-- Reuses int_delivered_orders_with_delay (built once, for Q1) instead of
-- recomputing delay_days here too.

with bucketed as (

    select
        d.order_id,
        r.review_score,
        case
            when d.delay_days <= 0 then 'early_or_on_time'
            when d.delay_days between 1 and 3 then '1_3_days_late'
            when d.delay_days between 4 and 7 then '4_7_days_late'
            else '8_plus_days_late'
        end as delay_bucket

    from {{ ref('int_delivered_orders_with_delay') }} d
    join {{ ref('stg_order_reviews') }} r on r.order_id = d.order_id

)

select
    delay_bucket,
    count(*) as order_count,
    round(avg(review_score), 2) as avg_review_score

from bucketed
group by delay_bucket
