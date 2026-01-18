{{
    config(
        materialized='table'
    )
}}

with fct_orders as (
    select * from {{ ref('fct_orders') }}
    where is_delivered = true
),

-- Monthly aggregation for Executive Dashboard
monthly_metrics as (
    select
        date_trunc('month', order_purchase_timestamp)::date as metric_month,
        
        -- MAIN KPI: Total Revenue (GMV)
        sum(order_total_value) as total_revenue_gmv,
        
        -- Additional KPIs
        count(*) as total_order_volume,
        round(sum(order_total_value) / nullif(count(*), 0), 2) as average_order_value,
        round(
            sum(case when is_perfect_order then 1 else 0 end) / nullif(count(*), 0),
            4
        ) as perfect_order_rate,
        count(distinct customer_unique_id) as total_active_customers

    from fct_orders
    group by 1
)

select * from monthly_metrics
order by metric_month
