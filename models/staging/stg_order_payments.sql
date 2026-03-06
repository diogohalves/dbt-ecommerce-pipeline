with source as (
    select * from {{ ref('olist_order_payments_dataset') }}
),

renamed as (
    select
        order_id,
        payment_sequential                              as payment_sequence,
        payment_type,
        payment_installments                            as installments,
        payment_value::numeric                          as payment_value
    from source
    where order_id is not null
)

select * from renamed