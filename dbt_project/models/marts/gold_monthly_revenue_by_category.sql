-- QUESTION 2 — build this file ONLY if you chose dbt/ELT for this question.
-- (If you chose PySpark instead, do this in a notebook and delete/ignore this file.)
--
-- Required output columns:
--   order_month           -- first day of month, e.g. 2018-01-01 (date)
--   category_english      -- product_category_name_english
--   revenue               -- sum(price) across order_items, rounded to 2 decimals
--   order_count            -- count of distinct orders

with items as (

    select
        oi.order_id,
        oi.price,
        p.product_category_name,
        o.order_purchase_ts

    from {{ ref('stg_order_items') }} oi
    join {{ ref('stg_products') }} p on p.product_id = oi.product_id
    join {{ ref('stg_orders') }} o on o.order_id = oi.order_id
    where o.order_status != 'canceled'

),

translated as (

    select
        items.*,
        cat.product_category_name_english as category_english
    from items
    left join {{ ref('stg_category_translation') }} cat
        on cat.product_category_name = items.product_category_name

)

select
    -- TODO: truncate order_purchase_ts to the first day of its month
    null as order_month,

    coalesce(category_english, 'unknown') as category_english,

    -- TODO: sum(price), rounded to 2 decimals
    null as revenue,

    -- TODO: count of DISTINCT order_id
    null as order_count

from translated
group by 1, 2
order by 1, 2
