with total_orders as (
    select 
        timestamp_trunc(created_at, week) as week_date
        , sum(total) as weekly_total_revenue
        , order_items.product_id
    from {{ source('coffee_shop', 'orders')}} orders
    left join {{ source('coffee_shop', 'order_items')}} order_items on orders.id = order_items.order_id
    group by 1,3
),

product_category as (
    select total_orders.weekly_total_revenue
        , total_orders.week_date
        , category as product_category
    from {{ source('coffee_shop', 'products')}} products
    left join total_orders on total_orders.product_id = products.id
)

select * 
from product_category
GROUP BY 1,2,3
ORDER BY 1