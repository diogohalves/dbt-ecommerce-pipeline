with customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select
        customer_id,
        count(order_id)                         as total_orders,
        min(purchased_at)                       as first_order_at,
        max(purchased_at)                       as last_order_at
    from {{ ref('stg_orders') }}
    group by customer_id
)

select
    c.customer_id,
    c.customer_unique_id,
    c.city,
    c.state,
    c.zip_code,
    coalesce(o.total_orders, 0)                 as total_orders,
    o.first_order_at,
    o.last_order_at
from customers c
left join orders o using (customer_id)