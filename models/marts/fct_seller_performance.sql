{{
    config(
        materialized='table'
    )
}}

with items_enriched as (
    select * from {{ ref('int_order_items_enriched') }}
),

orders as (
    select * from {{ ref('stg_olist__orders') }}
),

-- Get order dates for time-based aggregation
items_with_dates as (
    select
        i.*,
        date_trunc('month', o.order_purchase_timestamp)::date as order_month
    from items_enriched i
    inner join orders o on i.order_id = o.order_id
),

-- Monthly aggregation of seller/supply metrics
monthly_metrics as (
    select
        order_month,
        
        -- Active Sellers Count
        count(distinct seller_id) as active_sellers_count,
        
        -- Catalog Breadth
        count(distinct product_category_name_english) as catalog_breadth,
        
        -- Seller Geo-Coverage
        count(distinct seller_city) as seller_geo_coverage,
        
        -- Additional context
        count(distinct product_id) as active_products_count,
        count(*) as total_items_sold

    from items_with_dates
    group by 1
)

select * from monthly_metrics
order by order_month
