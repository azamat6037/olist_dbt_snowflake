{{
    config(
        materialized='table'
    )
}}

with fct_orders as (
    select * from {{ ref('fct_orders') }}
    where is_delivered = true
),

-- Monthly aggregation for Operations & Logistics
monthly_metrics as (
    select
        date_trunc('month', order_purchase_timestamp)::date as metric_month,
        
        count(*) as total_delivered_orders,
        
        -- MAIN KPI: On-Time Delivery Rate
        round(
            sum(case when is_on_time then 1 else 0 end) / nullif(count(*), 0),
            4
        ) as on_time_delivery_rate,
        
        -- Average Delivery Time (days)
        round(avg(delivery_time_days), 2) as avg_delivery_time_days,
        
        -- Freight Ratio (Cost Efficiency)
        round(
            sum(order_freight_value) / nullif(sum(order_items_value), 0),
            4
        ) as avg_freight_ratio,
        
        -- Carrier Handover Lag (hours from approval to carrier pickup)
        round(avg(processing_time_hours), 2) as avg_carrier_handover_lag_hours

    from fct_orders
    group by 1
)

select * from monthly_metrics
order by metric_month
