with total_orders as (
    select 
        timestamp_trunc(created_at, week) as week_of
        , sum(total) as weekly_total_revenue
        , order_items.product_id
    from `analytics-engineers-club.coffee_shop.orders` orders
    left join `analytics-engineers-club.coffee_shop.order_items` order_items on orders.id = order_items.order_id
    group by 1,3
),

product_category as (
    select total_orders.weekly_total_revenue
        , total_orders.week_of
        , category as product_category
    from `analytics-engineers-club.coffee_shop.products` products
    left join total_orders on total_orders.product_id = products.id
)

select * 
from product_category
GROUP BY 1,2,3
ORDER BY 1