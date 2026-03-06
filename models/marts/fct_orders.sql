with orders as (
    select * from {{ ref('stg_orders') }}
),

items as (
    select
        order_id,
        count(item_sequence)                    as total_items,
        sum(price)                              as items_value,
        sum(freight_value)                      as freight_value,
        sum(total_item_value)                   as gross_value
    from {{ ref('stg_order_items') }}
    group by order_id
),

payments as (
    select
        order_id,
        sum(payment_value)                      as payment_value,
        max(payment_type)                       as main_payment_type,
        max(installments)                       as max_installments
    from {{ ref('stg_order_payments') }}
    group by order_id
),

exchange as (
    select rate
    from {{ ref('exchange_rates') }}
    where target_currency = 'USD'
    limit 1
)

select
    o.order_id,
    o.customer_id,
    o.status,
    o.purchased_at,
    o.approved_at,
    o.shipped_at,
    o.delivered_at,
    o.estimated_delivery_at,
    coalesce(i.total_items, 0)                  as total_items,
    coalesce(i.items_value, 0)                  as items_value_brl,
    coalesce(i.freight_value, 0)                as freight_value_brl,
    coalesce(i.gross_value, 0)                  as gross_value_brl,
    coalesce(p.payment_value, 0)                as payment_value_brl,
    p.main_payment_type,
    p.max_installments,
    -- conversão cambial via API
    round((coalesce(i.gross_value, 0) * x.rate)::numeric, 2)  as gross_value_usd,
    round((coalesce(p.payment_value, 0) * x.rate)::numeric, 2) as payment_value_usd,
    x.rate                                      as usd_rate
from orders o
left join items i using (order_id)
left join payments p using (order_id)
cross join exchange x