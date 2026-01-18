{{
    config(
        materialized='table'
    )
}}

with items as (
    select * from {{ ref('stg_olist__items') }}
),

orders as (
    select * from {{ ref('stg_olist__orders') }}
),

dim_products as (
    select * from {{ ref('dim_products') }}
),

dim_sellers as (
    select * from {{ ref('dim_sellers') }}
),

final as (
    select
        -- Composite primary key
        i.order_id,
        i.order_item_id,
        
        -- Foreign keys
        dp.product_key,
        ds.seller_key,
        cast(to_char(o.order_purchase_timestamp::date, 'YYYYMMDD') as integer) as order_date_key,
        
        -- Natural keys (for convenience)
        i.product_id,
        i.seller_id,
        
        -- Order context
        o.order_status,
        o.order_purchase_timestamp,
        
        -- Product attributes (denormalized for convenience)
        dp.product_category_name_english,
        
        -- Seller attributes (denormalized for convenience)
        ds.seller_city,
        ds.seller_state,
        
        -- Measures
        i.price,
        i.freight_value,
        i.price + i.freight_value as total_value,
        
        -- Freight ratio
        case 
            when i.price > 0 then i.freight_value / i.price 
            else 0 
        end as freight_ratio,
        
        -- Shipping
        i.shipping_limit_date

    from items i
    inner join orders o on i.order_id = o.order_id
    left join dim_products dp on i.product_id = dp.product_id
    left join dim_sellers ds on i.seller_id = ds.seller_id
)

select * from final
