-- Este teste FALHA se encontrar algum pedido com valor negativo
select
    order_id,
    gross_value_brl
from {{ ref('fct_orders') }}
where gross_value_brl < 0