{{
    config(
        materialized='view'
    )
}}

with items as (
    select * from {{ ref('stg_olist__items') }}
),

order_items_agg as (
    select
        order_id,
        sum(price) as total_items_price,
        sum(freight_value) as total_freight_value,
        sum(price) + sum(freight_value) as total_order_value,
        count(*) as items_count,
        count(distinct product_id) as unique_products_count,
        count(distinct seller_id) as unique_sellers_count,
        avg(price) as avg_item_price,
        avg(freight_value) as avg_freight_value,
        case 
            when sum(price) > 0 then sum(freight_value) / sum(price) 
            else 0 
        end as freight_ratio,
        min(shipping_limit_date) as earliest_shipping_limit,
        max(shipping_limit_date) as latest_shipping_limit
    from items
    group by 1
)

select * from order_items_agg
