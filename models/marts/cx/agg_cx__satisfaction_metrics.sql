{{
    config(
        materialized='table'
    )
}}

with fct_orders as (
    select * from {{ ref('fct_orders') }}
),

dim_customers as (
    select * from {{ ref('dim_customers') }}
),

-- Monthly aggregation for Customer Experience (CX & Retention)
monthly_metrics as (
    select
        date_trunc('month', o.order_purchase_timestamp)::date as metric_month,
        
        count(*) as total_orders,
        
        -- MAIN KPI: Customer Satisfaction Score (CSAT) - Average review score
        round(avg(o.review_score), 2) as customer_satisfaction_score,
        
        -- Order Cancellation Rate
        round(
            sum(case when o.is_canceled then 1 else 0 end) / 
            nullif(count(*), 0),
            4
        ) as order_cancellation_rate,
        
        -- Repeat Buyer Rate (% of orders from repeat customers)
        round(
            sum(case when dc.is_repeat_buyer then 1 else 0 end) / 
            nullif(count(*), 0),
            4
        ) as repeat_buyer_rate,
        
        -- Response Rate (% of orders with reviews)
        round(
            sum(case when o.review_score is not null then 1 else 0 end) / 
            nullif(count(*), 0),
            4
        ) as response_rate,
        
        -- Additional context
        sum(case when o.review_score = 5 then 1 else 0 end) as five_star_reviews,
        sum(case when o.review_score = 1 then 1 else 0 end) as one_star_reviews,
        count(distinct o.customer_unique_id) as unique_customers

    from fct_orders o
    left join dim_customers dc on o.customer_key = dc.customer_key
    group by 1
)

select * from monthly_metrics
order by metric_month
