{{
    config(
        materialized='table'
    )
}}

with fct_order_items as (
    select * from {{ ref('fct_order_items') }}
    where order_status = 'delivered'
),

-- Seller revenue for concentration risk calculation
seller_revenue as (
    select
        date_trunc('month', order_purchase_timestamp)::date as order_month,
        seller_id,
        sum(price) as seller_revenue
    from fct_order_items
    group by 1, 2
),

-- Monthly total revenue
monthly_total as (
    select
        order_month,
        sum(seller_revenue) as total_revenue
    from seller_revenue
    group by 1
),

-- Top seller concentration (top 10% revenue share)
seller_ranked as (
    select
        sr.order_month,
        sr.seller_id,
        sr.seller_revenue,
        mt.total_revenue,
        percent_rank() over (partition by sr.order_month order by sr.seller_revenue desc) as revenue_percentile
    from seller_revenue sr
    join monthly_total mt on sr.order_month = mt.order_month
),

top_seller_concentration as (
    select
        order_month,
        round(
            sum(seller_revenue) / nullif(max(total_revenue), 0),
            4
        ) as top_seller_concentration_risk
    from seller_ranked
    where revenue_percentile <= 0.10  -- Top 10%
    group by 1
),

-- Monthly aggregation for Commercial (Sellers & Supply)
monthly_base_metrics as (
    select
        date_trunc('month', order_purchase_timestamp)::date as metric_month,
        
        -- MAIN KPI: Active Seller Count
        count(distinct seller_id) as active_seller_count,
        
        -- Catalog Breadth (unique categories sold)
        count(distinct product_category_name_english) as catalog_breadth,
        
        -- Seller Geographic Spread (unique seller cities)
        count(distinct seller_city) as seller_geographic_spread,
        
        -- Additional context
        count(distinct product_id) as active_products_count,
        count(*) as total_items_sold

    from fct_order_items
    group by 1
),

final as (
    select
        m.*,
        coalesce(t.top_seller_concentration_risk, 0) as top_seller_concentration_risk
    from monthly_base_metrics m
    left join top_seller_concentration t on m.metric_month = t.order_month
)

select * from final
order by metric_month
