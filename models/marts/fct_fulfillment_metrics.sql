{{
    config(
        materialized='table'
    )
}}

with orders_enriched as (
    select * from {{ ref('int_orders_enriched') }}
),

items_enriched as (
    select * from {{ ref('int_order_items_enriched') }}
),

-- Order-level freight metrics
order_freight as (
    select
        order_id,
        sum(price) as total_price,
        sum(freight_value) as total_freight,
        avg(freight_cost_pct) as avg_freight_cost_pct
    from items_enriched
    group by 1
),

-- Join orders with freight
orders_with_freight as (
    select
        o.*,
        f.total_price,
        f.total_freight,
        f.avg_freight_cost_pct
    from orders_enriched o
    left join order_freight f on o.order_id = f.order_id
),

-- Monthly aggregation of fulfillment metrics
monthly_metrics as (
    select
        date_trunc('month', order_purchase_timestamp)::date as order_month,
        
        count(*) as total_orders,
        
        -- Order Approval Speed (hours)
        round(avg(order_approval_hours), 2) as avg_order_approval_hours,
        
        -- Seller Processing Time (hours)
        round(avg(seller_processing_hours), 2) as avg_seller_processing_hours,
        
        -- Last Mile Duration (hours)
        round(avg(last_mile_hours), 2) as avg_last_mile_hours,
        
        -- Total Fulfillment Time (hours)
        round(avg(order_approval_hours + seller_processing_hours + last_mile_hours), 2) as avg_total_fulfillment_hours,
        
        -- Freight Cost Efficiency
        round(avg(avg_freight_cost_pct), 4) as avg_freight_cost_pct

    from orders_with_freight
    where order_status = 'delivered'
    group by 1
)

select * from monthly_metrics
order by order_month
