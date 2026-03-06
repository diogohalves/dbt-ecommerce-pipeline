with source as (
    select * from {{ ref('olist_order_items_dataset') }}
),

renamed as (
    select
        order_id,
        order_item_id                                   as item_sequence,
        product_id,
        seller_id,
        price::numeric                                  as price,
        freight_value::numeric                          as freight_value,
        (price + freight_value)::numeric                as total_item_value,
        shipping_limit_date::timestamp                  as shipping_limit_at
    from source
    where order_id is not null
)

select * from renamed