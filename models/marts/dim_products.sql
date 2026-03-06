with products as (
    select * from {{ ref('olist_products_dataset') }}
),

translations as (
    select * from {{ ref('product_category_name_translation') }}
),

items as (
    select
        product_id,
        count(distinct order_id)                as times_ordered,
        avg(price)                              as avg_price,
        sum(total_item_value)                   as total_revenue
    from {{ ref('stg_order_items') }}
    group by product_id
)

select
    p.product_id,
    coalesce(t.product_category_name_english, p.product_category_name, 'uncategorized') as category_en,
    p.product_category_name                     as category_pt,
    p.product_weight_g                          as weight_g,
    p.product_length_cm                         as length_cm,
    p.product_height_cm                         as height_cm,
    p.product_width_cm                          as width_cm,
    coalesce(i.times_ordered, 0)                as times_ordered,
    coalesce(i.avg_price, 0)                    as avg_price,
    coalesce(i.total_revenue, 0)                as total_revenue
from products p
left join translations t
    on p.product_category_name = t.product_category_name_english
left join items i using (product_id)