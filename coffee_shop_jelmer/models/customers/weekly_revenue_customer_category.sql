with orders_customers as (
  select created_at
  , timestamp_trunc(created_at, week) as week_date
  , id as order_id
  , customer_id
  , case when dense_rank() over(partition by customer_id ORDER BY created_at) = 1 then 'new' else 'returning' end as customer_type
from {{ source('coffee_shop', 'orders')}} orders
),

orders_price as (
  select order_id
  , order_items.product_id as product_id
  , product_prices.price as price
from {{ source('coffee_shop', 'order_items')}} order_items
left join {{ source('coffee_shop', 'product_prices')}} product_prices on order_items.product_id = product_prices.product_id
)

select orders_customers.week_date
  , orders_customers.customer_type
  , sum(orders_price.price) as total_per_week
from orders_customers
left join orders_price on orders_customers.order_id = orders_price.order_id
group by 1,2
order by 1,2