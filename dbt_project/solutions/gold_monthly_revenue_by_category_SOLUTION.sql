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
    date_trunc('month', order_purchase_ts) as order_month,
    coalesce(category_english, 'unknown')  as category_english,
    round(sum(price), 2)                   as revenue,
    count(distinct order_id)               as order_count

from translated
group by 1, 2
order by 1, 2
