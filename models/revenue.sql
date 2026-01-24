{{
    config(
        materialized='table'
    )
}}

WITH  order_value as (
    select
        order_id,
        sum(price) as sum_price,
        sum(freight_value) as sum_fv
    from
        {{ref('stg__items')}}
    group by all
)

select
    o.*,
    v.sum_price,
    v.sum_fv
from 
    {{ref('stg__orders')}} as o
left join
    order_value as v
    on o.order_id = v.order_id