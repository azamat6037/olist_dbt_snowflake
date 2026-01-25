{{
    config(
        materialized='view'
    )
}}

{#
    Intermediate model: Aggregate order item data at the order level.
    
    This model centralizes order item aggregation logic that was previously
    embedded in fct_orders, following dbt best practices for the intermediate layer.
#}

with items as (
    select * from {{ ref('stg_olist__items') }}
),

-- Aggregate items per order
order_items_agg as (
    select
        order_id,
        
        -- Revenue metrics
        sum(price) as total_items_price,
        sum(freight_value) as total_freight_value,
        sum(price) + sum(freight_value) as total_order_value,
        
        -- Count metrics
        count(*) as items_count,
        count(distinct product_id) as unique_products_count,
        count(distinct seller_id) as unique_sellers_count,
        
        -- Average metrics
        avg(price) as avg_item_price,
        avg(freight_value) as avg_freight_value,
        
        -- Freight ratio (cost efficiency)
        case 
            when sum(price) > 0 then sum(freight_value) / sum(price) 
            else 0 
        end as freight_ratio,
        
        -- Shipping timeline
        min(shipping_limit_date) as earliest_shipping_limit,
        max(shipping_limit_date) as latest_shipping_limit

    from items
    group by 1
)

select * from order_items_agg
